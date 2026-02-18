use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use tokio::io::AsyncBufReadExt;

use crate::error::TranscoderError;
use crate::progress::ProgressContext;
use crate::settings::{HwAccelMode, SettingsStore};

// ---------------------------------------------------------------------------
// FFmpeg / ffprobe installation info
// ---------------------------------------------------------------------------

/// Detected FFmpeg installation details.
#[derive(Debug, Clone, Default)]
pub struct FfmpegInfo {
    pub ffmpeg_found: bool,
    pub ffprobe_found: bool,
    pub ffmpeg_version: String,
    pub ffprobe_version: String,
    pub ffmpeg_path: String,
    pub ffprobe_path: String,
    pub encoders: Vec<String>,   // e.g. ["libx264", "aac", ...]
    pub hw_accels: Vec<String>,  // e.g. ["videotoolbox", "cuda"]
}

impl FfmpegInfo {
    pub fn is_ready(&self) -> bool {
        self.ffmpeg_found && self.ffprobe_found
    }
}

/// Detect FFmpeg and ffprobe installation, version, encoders and HW accels.
pub async fn detect() -> FfmpegInfo {
    let mut info = FfmpegInfo::default();

    // ── ffmpeg ───────────────────────────────────────────────────────
    if let Ok(out) = tokio::process::Command::new("ffmpeg")
        .args(["-version"])
        .output()
        .await
    {
        if out.status.success() {
            info.ffmpeg_found = true;
            let stdout = String::from_utf8_lossy(&out.stdout);
            // First line: "ffmpeg version N.N.N ..."
            if let Some(line) = stdout.lines().next() {
                info.ffmpeg_version = line
                    .strip_prefix("ffmpeg version ")
                    .unwrap_or(line)
                    .split_whitespace()
                    .next()
                    .unwrap_or("unknown")
                    .to_string();
            }
        }
    }

    // ffmpeg path via `which`
    if let Ok(out) = tokio::process::Command::new("which")
        .arg("ffmpeg")
        .output()
        .await
    {
        if out.status.success() {
            info.ffmpeg_path = String::from_utf8_lossy(&out.stdout).trim().to_string();
        }
    }

    // ── ffprobe ──────────────────────────────────────────────────────
    if let Ok(out) = tokio::process::Command::new("ffprobe")
        .args(["-version"])
        .output()
        .await
    {
        if out.status.success() {
            info.ffprobe_found = true;
            let stdout = String::from_utf8_lossy(&out.stdout);
            if let Some(line) = stdout.lines().next() {
                info.ffprobe_version = line
                    .strip_prefix("ffprobe version ")
                    .unwrap_or(line)
                    .split_whitespace()
                    .next()
                    .unwrap_or("unknown")
                    .to_string();
            }
        }
    }

    if let Ok(out) = tokio::process::Command::new("which")
        .arg("ffprobe")
        .output()
        .await
    {
        if out.status.success() {
            info.ffprobe_path = String::from_utf8_lossy(&out.stdout).trim().to_string();
        }
    }

    // ── Encoders (video + audio) ────────────────────────────────────
    if info.ffmpeg_found {
        if let Ok(out) = tokio::process::Command::new("ffmpeg")
            .args(["-hide_banner", "-encoders"])
            .output()
            .await
        {
            let stdout = String::from_utf8_lossy(&out.stdout);
            let important = [
                "libx264", "libx265", "libvpx", "libvpx-vp9", "libaom-av1",
                "libsvtav1", "h264_videotoolbox", "hevc_videotoolbox",
                "h264_nvenc", "hevc_nvenc", "h264_qsv", "hevc_qsv",
                "h264_vaapi", "hevc_vaapi", "aac", "libfdk_aac", "libopus",
                "libmp3lame", "libvorbis",
            ];
            for line in stdout.lines() {
                let trimmed = line.trim();
                for enc in &important {
                    if trimmed.contains(enc) && !info.encoders.contains(&enc.to_string()) {
                        info.encoders.push(enc.to_string());
                    }
                }
            }
        }

        // ── HW accels ───────────────────────────────────────────────
        if let Ok(out) = tokio::process::Command::new("ffmpeg")
            .args(["-hide_banner", "-hwaccels"])
            .output()
            .await
        {
            let stdout = String::from_utf8_lossy(&out.stdout);
            for line in stdout.lines().skip(1) {
                let name = line.trim();
                if !name.is_empty() {
                    info.hw_accels.push(name.to_string());
                }
            }
        }
    }

    info
}

// ---------------------------------------------------------------------------
// Resolved encoder configuration (GPU or CPU)
// ---------------------------------------------------------------------------

/// The resolved encoder to use for a transcode job.
#[derive(Debug, Clone)]
pub struct ResolvedEncoder {
    /// e.g. "libx264", "h264_videotoolbox", "h264_nvenc"
    pub codec: String,
    /// Additional encoder-specific args injected before the output.
    pub extra_args: Vec<String>,
    /// Human label for logging.
    pub label: String,
    /// True if this is a hardware encoder.
    pub is_hw: bool,
}

impl ResolvedEncoder {
    fn cpu() -> Self {
        Self {
            codec: "libx264".into(),
            extra_args: vec!["-preset".into(), "medium".into()],
            label: "CPU (libx264)".into(),
            is_hw: false,
        }
    }

    fn videotoolbox() -> Self {
        Self {
            codec: "h264_videotoolbox".into(),
            // VideoToolbox doesn't use -preset; use -q:v for quality (lower = better)
            extra_args: vec![
                "-profile:v".into(), "high".into(),
                "-level".into(), "4.1".into(),
                "-allow_sw".into(), "1".into(), // fallback to SW if HW busy
            ],
            label: "Apple VideoToolbox (GPU)".into(),
            is_hw: true,
        }
    }

    fn nvenc() -> Self {
        Self {
            codec: "h264_nvenc".into(),
            extra_args: vec![
                "-preset".into(), "p4".into(), // balanced quality/speed
                "-tune".into(), "hq".into(),
                "-rc".into(), "vbr".into(),
            ],
            label: "NVIDIA NVENC (GPU)".into(),
            is_hw: true,
        }
    }

    fn qsv() -> Self {
        Self {
            codec: "h264_qsv".into(),
            extra_args: vec![
                "-preset".into(), "medium".into(),
            ],
            label: "Intel QSV (GPU)".into(),
            is_hw: true,
        }
    }

    fn vaapi() -> Self {
        Self {
            codec: "h264_vaapi".into(),
            extra_args: vec![],
            label: "VA-API (GPU)".into(),
            is_hw: true,
        }
    }
}

/// Resolve the actual encoder from the configured `HwAccelMode` and the
/// detected FFmpeg installation info.
///
/// In "Auto" mode the priority order is:
///   videotoolbox > nvenc > qsv > vaapi > libx264
pub fn resolve_encoder(mode: &HwAccelMode, ff: &FfmpegInfo) -> ResolvedEncoder {
    match mode {
        HwAccelMode::Cpu => ResolvedEncoder::cpu(),
        HwAccelMode::Videotoolbox => {
            if ff.encoders.contains(&"h264_videotoolbox".to_string()) {
                ResolvedEncoder::videotoolbox()
            } else {
                warn!("h264_videotoolbox not available, falling back to CPU");
                ResolvedEncoder::cpu()
            }
        }
        HwAccelMode::Nvenc => {
            if ff.encoders.contains(&"h264_nvenc".to_string()) {
                ResolvedEncoder::nvenc()
            } else {
                warn!("h264_nvenc not available, falling back to CPU");
                ResolvedEncoder::cpu()
            }
        }
        HwAccelMode::Qsv => {
            if ff.encoders.contains(&"h264_qsv".to_string()) {
                ResolvedEncoder::qsv()
            } else {
                warn!("h264_qsv not available, falling back to CPU");
                ResolvedEncoder::cpu()
            }
        }
        HwAccelMode::Vaapi => {
            if ff.encoders.contains(&"h264_vaapi".to_string()) {
                ResolvedEncoder::vaapi()
            } else {
                warn!("h264_vaapi not available, falling back to CPU");
                ResolvedEncoder::cpu()
            }
        }
        HwAccelMode::Auto => {
            // Priority: videotoolbox > nvenc > qsv > vaapi > CPU
            if ff.encoders.contains(&"h264_videotoolbox".to_string()) {
                info!("Auto-detected Apple VideoToolbox GPU encoder");
                ResolvedEncoder::videotoolbox()
            } else if ff.encoders.contains(&"h264_nvenc".to_string()) {
                info!("Auto-detected NVIDIA NVENC GPU encoder");
                ResolvedEncoder::nvenc()
            } else if ff.encoders.contains(&"h264_qsv".to_string()) {
                info!("Auto-detected Intel QSV GPU encoder");
                ResolvedEncoder::qsv()
            } else if ff.encoders.contains(&"h264_vaapi".to_string()) {
                info!("Auto-detected VA-API GPU encoder");
                ResolvedEncoder::vaapi()
            } else {
                info!("No GPU encoder found, using CPU (libx264)");
                ResolvedEncoder::cpu()
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoInfo {
    pub width: u32,
    pub height: u32,
    pub duration_seconds: f64,
    pub has_audio: bool,
    pub codec: String,
}

#[derive(Debug, Clone)]
pub struct Rendition {
    pub name: String,
    pub width: u32,
    pub height: u32,
    pub video_bitrate: String,
    pub audio_bitrate: String,
    pub bandwidth: u64,
}

impl Rendition {
    /// Build renditions from the settings store's enabled presets, filtered
    /// to those whose height ≤ the source height. Always keeps the smallest
    /// enabled rendition so there is always *some* output.
    pub fn for_source_from_settings(
        info: &VideoInfo,
        enabled_names: &[String],
    ) -> Vec<Self> {
        use crate::settings::RenditionPreset;

        let all = RenditionPreset::all_defaults();

        // Build Rendition from each enabled preset
        let enabled: Vec<Self> = all
            .into_iter()
            .filter(|p| enabled_names.iter().any(|n| n.eq_ignore_ascii_case(&p.name)))
            .map(|p| Self {
                name: p.name,
                width: p.width,
                height: p.height,
                video_bitrate: p.video_bitrate,
                audio_bitrate: p.audio_bitrate,
                bandwidth: p.bandwidth,
            })
            .collect();

        if enabled.is_empty() {
            // Fallback: at least 360p
            return vec![Self {
                name: "360p".into(),
                width: 640,
                height: 360,
                video_bitrate: "800k".into(),
                audio_bitrate: "96k".into(),
                bandwidth: 896_000,
            }];
        }

        let min_height = enabled.iter().map(|r| r.height).min().unwrap_or(360);

        enabled
            .into_iter()
            .filter(|r| r.height <= info.height || r.height <= min_height)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// ffprobe
// ---------------------------------------------------------------------------

pub async fn probe(input: &Path) -> Result<VideoInfo, TranscoderError> {
    let output = Command::new("ffprobe")
        .args(["-v", "quiet", "-print_format", "json", "-show_format", "-show_streams"])
        .arg(input)
        .output()
        .await
        .map_err(|e| TranscoderError::Transcode(format!("ffprobe spawn: {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(TranscoderError::Transcode(format!("ffprobe: {stderr}")));
    }

    let json: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|e| TranscoderError::Transcode(format!("ffprobe json: {e}")))?;

    let streams = json["streams"]
        .as_array()
        .ok_or_else(|| TranscoderError::Transcode("no streams".into()))?;

    let video = streams
        .iter()
        .find(|s| s["codec_type"].as_str() == Some("video"))
        .ok_or_else(|| TranscoderError::InvalidFormat("no video stream".into()))?;

    let has_audio = streams
        .iter()
        .any(|s| s["codec_type"].as_str() == Some("audio"));

    let width = video["width"].as_u64().unwrap_or(0) as u32;
    let height = video["height"].as_u64().unwrap_or(0) as u32;
    let duration_seconds: f64 = json["format"]["duration"]
        .as_str()
        .unwrap_or("0")
        .parse()
        .unwrap_or(0.0);
    let codec = video["codec_name"]
        .as_str()
        .unwrap_or("unknown")
        .to_string();

    Ok(VideoInfo {
        width,
        height,
        duration_seconds,
        has_audio,
        codec,
    })
}

// ---------------------------------------------------------------------------
// Single rendition → HLS
// ---------------------------------------------------------------------------

pub async fn transcode_rendition(
    input: &Path,
    output_dir: &Path,
    rendition: &Rendition,
    has_audio: bool,
    cancel_token: &CancellationToken,
    encoder: &ResolvedEncoder,
    progress: Option<&ProgressContext>,
) -> Result<(), TranscoderError> {
    let rdir = output_dir.join(&rendition.name);
    tokio::fs::create_dir_all(&rdir)
        .await
        .map_err(|e| TranscoderError::Transcode(format!("mkdir: {e}")))?;

    let playlist = rdir.join("playlist.m3u8");
    let seg_pat = rdir.join("segment_%03d.ts");

    // ── Build video filter chain ────────────────────────────────────
    let vf = if encoder.codec == "h264_vaapi" {
        format!(
            "format=nv12,hwupload,scale_vaapi=w={}:h={}",
            rendition.width, rendition.height,
        )
    } else {
        format!(
            "scale={}:{}:force_original_aspect_ratio=decrease,\
             pad={}:{}:(ow-iw)/2:(oh-ih)/2",
            rendition.width, rendition.height, rendition.width, rendition.height,
        )
    };

    let mut args: Vec<String> = Vec::with_capacity(48);
    args.extend(["-y".into()]);

    if encoder.codec == "h264_vaapi" {
        args.extend(["-vaapi_device".into(), "/dev/dri/renderD128".into()]);
    }

    args.extend([
        "-i".into(),
        input.to_string_lossy().into(),
        "-vf".into(),
        vf,
        "-c:v".into(),
        encoder.codec.clone(),
        "-b:v".into(),
        rendition.video_bitrate.clone(),
    ]);

    args.extend(encoder.extra_args.iter().cloned());

    args.extend(["-g".into(), "48".into(), "-keyint_min".into(), "48".into()]);

    if !encoder.is_hw {
        args.extend(["-sc_threshold".into(), "0".into()]);
    }

    if has_audio {
        args.extend([
            "-c:a".into(), "aac".into(),
            "-b:a".into(), rendition.audio_bitrate.clone(),
            "-ac".into(), "2".into(),
        ]);
    } else {
        args.push("-an".into());
    }

    args.extend([
        "-f".into(), "hls".into(),
        "-hls_time".into(), "6".into(),
        "-hls_list_size".into(), "0".into(),
        "-hls_playlist_type".into(), "vod".into(),
        "-hls_segment_filename".into(), seg_pat.to_string_lossy().into(),
    ]);

    // Write machine-readable progress to stdout (key=value lines)
    if progress.is_some() {
        args.extend(["-progress".into(), "pipe:1".into()]);
    }

    args.push(playlist.to_string_lossy().into());

    info!(rendition = %rendition.name, encoder = %encoder.codec, "Spawning ffmpeg");

    let mut child = Command::new("ffmpeg")
        .args(&args)
        .stdout(if progress.is_some() {
            std::process::Stdio::piped()
        } else {
            std::process::Stdio::null()
        })
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| TranscoderError::Transcode(format!("spawn ffmpeg: {e}")))?;

    // If progress tracking is enabled, read stdout line-by-line for progress
    // while also watching for cancellation. Stderr is collected separately.
    if let Some(ctx) = progress {
        let stdout = child.stdout.take().unwrap();
        let stderr_pipe = child.stderr.take().unwrap();

        // Collect stderr in a background task for error reporting
        let stderr_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut tokio::io::BufReader::new(stderr_pipe), &mut buf)
                .await
                .ok();
            String::from_utf8_lossy(&buf).to_string()
        });

        let reader = tokio::io::BufReader::new(stdout);
        let mut lines = reader.lines();
        let mut last_speed = String::new();

        loop {
            tokio::select! {
                biased;

                _ = cancel_token.cancelled() => {
                    warn!(rendition = %rendition.name, "Canceled – killing ffmpeg");
                    child.kill().await.ok();
                    return Err(TranscoderError::Canceled);
                }

                line_result = lines.next_line() => {
                    match line_result {
                        Ok(Some(line)) => {
                            // Parse key=value lines from -progress pipe:1
                            if let Some(val) = line.strip_prefix("out_time_us=") {
                                if let Ok(us) = val.trim().parse::<i64>() {
                                    let secs = us as f64 / 1_000_000.0;
                                    ctx.report(secs, &last_speed);
                                }
                            } else if let Some(val) = line.strip_prefix("speed=") {
                                last_speed = val.trim().to_string();
                            }
                        }
                        Ok(None) => break, // EOF — ffmpeg finished
                        Err(_) => break,
                    }
                }
            }
        }

        let status = child.wait().await
            .map_err(|e| TranscoderError::Transcode(format!("ffmpeg wait: {e}")))?;
        let stderr_output = stderr_task.await.unwrap_or_default();

        if !status.success() {
            let snippet: String = stderr_output.chars().take(500).collect();
            return Err(TranscoderError::Transcode(
                format!("ffmpeg {}: {snippet}", status),
            ));
        }
    } else {
        // No progress tracking — use the simpler wait_with_output approach
        tokio::select! {
            biased;

            _ = cancel_token.cancelled() => {
                warn!(rendition = %rendition.name, "Canceled – killing ffmpeg");
                return Err(TranscoderError::Canceled);
            }

            result = child.wait_with_output() => {
                let out = result.map_err(|e| TranscoderError::Transcode(format!("ffmpeg: {e}")))?;
                if !out.status.success() {
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    let snippet: String = stderr.chars().take(500).collect();
                    return Err(TranscoderError::Transcode(
                        format!("ffmpeg {}: {snippet}", out.status),
                    ));
                }
            }
        }
    }

    info!(rendition = %rendition.name, "Rendition done");
    Ok(())
}

// ---------------------------------------------------------------------------
// Master playlist generator
// ---------------------------------------------------------------------------

pub fn generate_master_playlist(
    output_dir: &Path,
    renditions: &[Rendition],
) -> Result<PathBuf, TranscoderError> {
    let mut content = String::from("#EXTM3U\n#EXT-X-VERSION:3\n\n");

    for r in renditions {
        content.push_str(&format!(
            "#EXT-X-STREAM-INF:BANDWIDTH={},RESOLUTION={}x{},NAME=\"{}\"\n\
             {}/playlist.m3u8\n\n",
            r.bandwidth, r.width, r.height, r.name, r.name,
        ));
    }

    let path = output_dir.join("master.m3u8");
    std::fs::write(&path, &content)
        .map_err(|e| TranscoderError::Transcode(format!("write master.m3u8: {e}")))?;
    Ok(path)
}

// ---------------------------------------------------------------------------
// Full HLS transcode pipeline
// ---------------------------------------------------------------------------

pub async fn transcode_to_hls(
    input: &Path,
    output_dir: &Path,
    cancel_token: &CancellationToken,
    settings: &SettingsStore,
    tracker: Option<&crate::progress::ProgressTracker>,
    video_id: i64,
    job_id: &str,
) -> Result<(Vec<Rendition>, VideoInfo), TranscoderError> {
    let info = probe(input).await?;
    info!(
        width = info.width,
        height = info.height,
        duration = info.duration_seconds,
        codec = %info.codec,
        "Source video probed"
    );

    let enabled_names = settings.enabled_rendition_names().await;
    let renditions = Rendition::for_source_from_settings(&info, &enabled_names);
    if renditions.is_empty() {
        return Err(TranscoderError::InvalidFormat(
            "No suitable renditions for source resolution".into(),
        ));
    }

    // Resolve the encoder (GPU or CPU) from settings + detected capabilities
    let hw_mode = settings.hw_accel().await;
    let ff = detect().await;
    let encoder = resolve_encoder(&hw_mode, &ff);
    info!(
        encoder = %encoder.codec,
        label = %encoder.label,
        hw = encoder.is_hw,
        "Using video encoder"
    );

    info!(count = renditions.len(), "Transcoding renditions");

    let total = renditions.len();
    for (idx, r) in renditions.iter().enumerate() {
        if cancel_token.is_cancelled() {
            return Err(TranscoderError::Canceled);
        }

        // Build per-rendition progress context
        let ctx = tracker.map(|t| ProgressContext {
            tracker: t.clone(),
            video_id,
            job_id: job_id.to_string(),
            duration_secs: info.duration_seconds,
            total_renditions: total,
            rendition_index: idx,
            rendition_name: r.name.clone(),
        });

        transcode_rendition(
            input, output_dir, r, info.has_audio, cancel_token, &encoder,
            ctx.as_ref(),
        )
        .await?;
    }

    generate_master_playlist(output_dir, &renditions)?;
    Ok((renditions, info))
}
