use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use sysinfo::{Disks, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Raw snapshot of system metrics (numbers only)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default, Serialize)]
pub struct SystemSnapshot {
    pub cpu_usage_pct: f32,
    pub cpu_count: usize,
    pub mem_total_bytes: u64,
    pub mem_used_bytes: u64,
    pub swap_total_bytes: u64,
    pub swap_used_bytes: u64,
    pub disk_total_bytes: u64,
    pub disk_used_bytes: u64,
    pub load_1: f64,
    pub load_5: f64,
    pub load_15: f64,
    pub proc_mem_bytes: u64,
    pub proc_cpu_pct: f32,
    pub sys_uptime_secs: u64,
    pub os_name: String,
    pub hostname: String,
}

// ---------------------------------------------------------------------------
// Display-ready struct with precomputed labels & CSS classes
// (passed directly to askama templates)
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub struct SystemDisplay {
    // CPU
    pub cpu_pct: f32,
    pub cpu_pct_label: String,
    pub cpu_width: String,
    pub cpu_color: &'static str,
    pub cpu_count: usize,
    // Memory
    pub mem_pct: f32,
    pub mem_pct_label: String,
    pub mem_used: String,
    pub mem_total: String,
    pub mem_width: String,
    pub mem_color: &'static str,
    // Swap
    pub swap_used: String,
    pub swap_total: String,
    // Disk
    pub disk_pct: f32,
    pub disk_pct_label: String,
    pub disk_used: String,
    pub disk_total: String,
    pub disk_width: String,
    pub disk_color: &'static str,
    // Load average
    pub load_1: String,
    pub load_5: String,
    pub load_15: String,
    // Process
    pub proc_mem: String,
    pub proc_cpu: String,
    // System
    pub uptime: String,
    pub os_name: String,
    pub hostname: String,
}

impl SystemSnapshot {
    pub fn to_display(&self) -> SystemDisplay {
        let mem_pct = pct(self.mem_used_bytes, self.mem_total_bytes);
        let disk_pct = pct(self.disk_used_bytes, self.disk_total_bytes);

        SystemDisplay {
            cpu_pct: self.cpu_usage_pct,
            cpu_pct_label: format!("{:.1}%", self.cpu_usage_pct),
            cpu_width: format!("{:.1}", self.cpu_usage_pct.min(100.0)),
            cpu_color: bar_color(self.cpu_usage_pct),
            cpu_count: self.cpu_count,

            mem_pct,
            mem_pct_label: format!("{:.1}%", mem_pct),
            mem_used: fmt_bytes(self.mem_used_bytes),
            mem_total: fmt_bytes(self.mem_total_bytes),
            mem_width: format!("{:.1}", mem_pct.min(100.0)),
            mem_color: bar_color(mem_pct),

            swap_used: fmt_bytes(self.swap_used_bytes),
            swap_total: fmt_bytes(self.swap_total_bytes),

            disk_pct,
            disk_pct_label: format!("{:.1}%", disk_pct),
            disk_used: fmt_bytes(self.disk_used_bytes),
            disk_total: fmt_bytes(self.disk_total_bytes),
            disk_width: format!("{:.1}", disk_pct.min(100.0)),
            disk_color: bar_color(disk_pct),

            load_1: format!("{:.2}", self.load_1),
            load_5: format!("{:.2}", self.load_5),
            load_15: format!("{:.2}", self.load_15),

            proc_mem: fmt_bytes(self.proc_mem_bytes),
            proc_cpu: format!("{:.1}%", self.proc_cpu_pct),

            uptime: fmt_uptime(self.sys_uptime_secs),
            os_name: self.os_name.clone(),
            hostname: self.hostname.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Background monitor that refreshes every 3 s
// ---------------------------------------------------------------------------

pub struct SystemMonitor {
    data: Arc<RwLock<SystemSnapshot>>,
}

impl SystemMonitor {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(SystemSnapshot::default())),
        }
    }

    /// Start the background refresh loop. Call once at startup.
    pub fn start(&self) {
        let data = Arc::clone(&self.data);
        tokio::spawn(async move {
            let mut sys = System::new_all();

            // Resolve our own PID once
            let my_pid = sysinfo::get_current_pid().ok();

            // First refresh needs a short delay for accurate CPU readings
            tokio::time::sleep(Duration::from_millis(500)).await;

            let os_name = System::long_os_version().unwrap_or_default();
            let hostname = System::host_name().unwrap_or_default();

            loop {
                // Refresh only what we need
                sys.refresh_cpu_usage();
                sys.refresh_memory();

                // Refresh our own process
                let (proc_mem, proc_cpu) = if let Some(pid) = my_pid {
                    sys.refresh_processes_specifics(
                        ProcessesToUpdate::Some(&[pid]),
                        false,
                        ProcessRefreshKind::nothing().with_cpu().with_memory(),
                    );
                    sys.process(pid)
                        .map(|p| (p.memory(), p.cpu_usage()))
                        .unwrap_or((0, 0.0))
                } else {
                    (0, 0.0)
                };

                // Disk – pick the largest partition
                let disks = Disks::new_with_refreshed_list();
                let (d_total, d_avail) = disks
                    .list()
                    .iter()
                    .max_by_key(|d| d.total_space())
                    .map(|d| (d.total_space(), d.available_space()))
                    .unwrap_or((0, 0));

                let load = System::load_average();

                let snap = SystemSnapshot {
                    cpu_usage_pct: sys.global_cpu_usage(),
                    cpu_count: sys.cpus().len(),
                    mem_total_bytes: sys.total_memory(),
                    mem_used_bytes: sys.used_memory(),
                    swap_total_bytes: sys.total_swap(),
                    swap_used_bytes: sys.used_swap(),
                    disk_total_bytes: d_total,
                    disk_used_bytes: d_total.saturating_sub(d_avail),
                    load_1: load.one,
                    load_5: load.five,
                    load_15: load.fifteen,
                    proc_mem_bytes: proc_mem,
                    proc_cpu_pct: proc_cpu,
                    sys_uptime_secs: System::uptime(),
                    os_name: os_name.clone(),
                    hostname: hostname.clone(),
                };

                *data.write().await = snap;
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
    }

    /// Read the latest snapshot (non-blocking).
    pub async fn snapshot(&self) -> SystemSnapshot {
        self.data.read().await.clone()
    }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn pct(used: u64, total: u64) -> f32 {
    if total == 0 {
        0.0
    } else {
        (used as f64 / total as f64 * 100.0) as f32
    }
}

fn bar_color(pct: f32) -> &'static str {
    if pct > 90.0 {
        "bg-red-500"
    } else if pct > 70.0 {
        "bg-yellow-500"
    } else {
        "bg-blue-500"
    }
}

fn fmt_bytes(bytes: u64) -> String {
    const GB: f64 = 1_073_741_824.0;
    const MB: f64 = 1_048_576.0;
    const KB: f64 = 1_024.0;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.1} GB", b / GB)
    } else if b >= MB {
        format!("{:.1} MB", b / MB)
    } else if b >= KB {
        format!("{:.0} KB", b / KB)
    } else {
        format!("{} B", bytes)
    }
}

fn fmt_uptime(secs: u64) -> String {
    let d = secs / 86400;
    let h = (secs % 86400) / 3600;
    let m = (secs % 3600) / 60;
    if d > 0 {
        format!("{}d {}h {}m", d, h, m)
    } else if h > 0 {
        format!("{}h {}m", h, m)
    } else {
        format!("{}m", m)
    }
}
