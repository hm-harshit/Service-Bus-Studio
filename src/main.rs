#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod mgmt;
mod worker;

use eframe::egui;
use egui::{Align2, Color32, RichText};
use egui_extras::{Column, TableBuilder};
use mgmt::Entity;
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use worker::{Cmd, Evt, MsgView, OutMessage, Target};

struct Palette {
    dark: bool,
    accent: Color32,
    btn: Color32, // primary action pill fill
    dlq_red: Color32,
    red_dim: Color32,
    ok_green: Color32,
    green_dim: Color32,
    amber: Color32,
    bg: Color32,
    panel: Color32,
    card: Color32,
    card_border: Color32,
    input_bg: Color32,
    selected_bg: Color32,
    text: Color32,
    text_weak: Color32,
}

// iOS-style warm light palette (cream surfaces, white glass cards, black pill buttons)
static LIGHT: Palette = Palette {
    dark: false,
    accent: Color32::from_rgb(0xd9, 0x77, 0x57),
    btn: Color32::from_rgb(0x16, 0x16, 0x14),
    dlq_red: Color32::from_rgb(0xc6, 0x28, 0x28),
    red_dim: Color32::from_rgb(0xfd, 0xec, 0xea),
    ok_green: Color32::from_rgb(0x2e, 0x7d, 0x32),
    green_dim: Color32::from_rgb(0xe8, 0xf5, 0xe9),
    amber: Color32::from_rgb(0xb3, 0x80, 0x1a),
    bg: Color32::from_rgb(0xf6, 0xec, 0xdf),
    panel: Color32::from_rgb(0xfb, 0xf5, 0xec),
    card: Color32::WHITE,
    card_border: Color32::from_rgb(0xea, 0xe2, 0xd5),
    input_bg: Color32::WHITE,
    selected_bg: Color32::from_rgb(0xea, 0xe2, 0xd3),
    text: Color32::from_rgb(0x1d, 0x1c, 0x1a),
    text_weak: Color32::from_rgb(0x8e, 0x8a, 0x82),
};

// Claude-style warm charcoal palette
static DARK: Palette = Palette {
    dark: true,
    accent: Color32::from_rgb(0xd9, 0x77, 0x57),
    btn: Color32::from_rgb(0xd9, 0x77, 0x57),
    dlq_red: Color32::from_rgb(0xe5, 0x73, 0x73),
    red_dim: Color32::from_rgb(0x3a, 0x22, 0x1f),
    ok_green: Color32::from_rgb(0x9c, 0xc0, 0x8f),
    green_dim: Color32::from_rgb(0x2a, 0x31, 0x26),
    amber: Color32::from_rgb(0xd9, 0xb1, 0x6c),
    bg: Color32::from_rgb(0x26, 0x26, 0x24),
    panel: Color32::from_rgb(0x1a, 0x1a, 0x18),
    card: Color32::from_rgb(0x30, 0x30, 0x2e),
    card_border: Color32::from_rgb(0x3d, 0x3d, 0x3a),
    input_bg: Color32::from_rgb(0x1f, 0x1f, 0x1d),
    selected_bg: Color32::from_rgb(0x3a, 0x3a, 0x38),
    text: Color32::from_rgb(0xf0, 0xef, 0xea),
    text_weak: Color32::from_rgb(0x9b, 0x9a, 0x96),
};

static USE_DARK: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn pal() -> &'static Palette {
    if USE_DARK.load(std::sync::atomic::Ordering::Relaxed) { &DARK } else { &LIGHT }
}

fn card_shadow() -> egui::Shadow {
    let alpha = if pal().dark { 70 } else { 16 };
    egui::Shadow { offset: [0, 3], blur: 12, spread: 0, color: Color32::from_black_alpha(alpha) }
}

fn serif() -> egui::FontFamily {
    egui::FontFamily::Name("serif".into())
}

fn main() -> eframe::Result {
    let icon = egui::IconData {
        rgba: include_bytes!("../assets/icon_64.rgba").to_vec(),
        width: 64,
        height: 64,
    };
    let options = eframe::NativeOptions {
        renderer: eframe::Renderer::Glow,
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1360.0, 860.0])
            .with_min_inner_size([900.0, 600.0])
            .with_icon(icon)
            .with_title(format!("Service Bus Explorer Advance {}", env!("CARGO_PKG_VERSION"))),
        ..Default::default()
    };
    eframe::run_native(
        "sbx",
        options,
        Box::new(|cc| Ok(Box::new(App::new(cc)))),
    )
}

#[derive(Clone, Debug, PartialEq)]
enum Sel {
    None,
    Queue(String),
    Topic(String),
    Sub { topic: String, name: String },
}

#[derive(Clone, Copy, PartialEq)]
enum Tab {
    Overview,
    Messages,
    DeadLetter,
    Send,
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct Config {
    connections: Vec<String>,
    #[serde(default)]
    dark: bool,
}

fn config_path() -> std::path::PathBuf {
    std::path::PathBuf::from(std::env::var("APPDATA").unwrap_or_else(|_| ".".into()))
        .join("sbx.json")
}

struct App {
    tx: Sender<Cmd>,
    rx: Receiver<Evt>,
    // connection
    conn_str: String,
    config: Config,
    connected_ns: Option<String>,
    busy: i32,
    status: String,
    status_is_error: bool,
    // entities
    queues: Vec<Entity>,
    topics: Vec<Entity>,
    subs: HashMap<String, Vec<Entity>>,
    filter: String,
    // selection / view
    sel: Sel,
    tab: Tab,
    messages: HashMap<(String, bool), Vec<MsgView>>,
    selected_row: Option<usize>,
    peek_max: u32,
    // send form
    out: OutMessage,
    // entity settings editor: (selection key, editable field values)
    edit: Option<(String, Vec<(String, String)>)>,
    // log
    log: Vec<(String, String, bool)>, // (time, message, is_error)
    show_log: bool,
    // dialogs
    new_entity: Option<(String, String, String)>, // (kind, topic, name)
    confirm: Option<(String, Cmd)>,
    show_about: bool,
    show_connect: bool,
}

fn now_hms_utc() -> String {
    let s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    format!("{:02}:{:02}:{:02}", (s / 3600) % 24, (s / 60) % 60, s % 60)
}

impl App {
    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        let config: Config = std::fs::read_to_string(config_path())
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        USE_DARK.store(config.dark, std::sync::atomic::Ordering::Relaxed);
        apply_style(&cc.egui_ctx);
        let (tx, rx) = worker::spawn(cc.egui_ctx.clone());
        Self {
            tx,
            rx,
            conn_str: config.connections.first().cloned().unwrap_or_default(),
            config,
            connected_ns: None,
            busy: 0,
            status: "Not connected. Paste a connection string and hit Connect.".into(),
            status_is_error: false,
            queues: Vec::new(),
            topics: Vec::new(),
            subs: HashMap::new(),
            filter: String::new(),
            sel: Sel::None,
            tab: Tab::Overview,
            messages: HashMap::new(),
            selected_row: None,
            peek_max: 32,
            out: OutMessage { count: 1, content_type: "application/json".into(), ..Default::default() },
            edit: None,
            log: Vec::new(),
            show_log: false,
            new_entity: None,
            confirm: None,
            show_about: false,
            show_connect: true, // open the connect dialog on launch
        }
    }

    fn save_config(&self) {
        let _ = std::fs::write(
            config_path(),
            serde_json::to_string_pretty(&self.config).unwrap(),
        );
    }

    fn set_theme(&mut self, dark: bool, ctx: &egui::Context) {
        USE_DARK.store(dark, std::sync::atomic::Ordering::Relaxed);
        self.config.dark = dark;
        self.save_config();
        apply_style(ctx);
    }

    fn disconnect(&mut self) {
        self.connected_ns = None;
        self.queues.clear();
        self.topics.clear();
        self.subs.clear();
        self.messages.clear();
        self.sel = Sel::None;
        self.edit = None;
        self.status = "Disconnected.".into();
        self.status_is_error = false;
    }

    fn send(&self, cmd: Cmd) {
        let _ = self.tx.send(cmd);
    }

    fn target(&self) -> Option<Target> {
        match &self.sel {
            Sel::Queue(q) => Some(Target::Queue(q.clone())),
            Sel::Sub { topic, name } => {
                Some(Target::Subscription { topic: topic.clone(), name: name.clone() })
            }
            _ => None,
        }
    }

    fn selected_entity(&self) -> Option<&Entity> {
        match &self.sel {
            Sel::Queue(q) => self.queues.iter().find(|e| &e.name == q),
            Sel::Topic(t) => self.topics.iter().find(|e| &e.name == t),
            Sel::Sub { topic, name } => {
                self.subs.get(topic)?.iter().find(|e| &e.name == name)
            }
            Sel::None => None,
        }
    }

    fn drain_events(&mut self) {
        while let Ok(evt) = self.rx.try_recv() {
            match evt {
                Evt::Connected(ns) => {
                    self.connected_ns = Some(ns);
                    self.config.connections.retain(|c| c != &self.conn_str);
                    self.config.connections.insert(0, self.conn_str.clone());
                    self.config.connections.truncate(10);
                    self.save_config();
                }
                Evt::Entities { queues, topics, subs } => {
                    self.queues = queues;
                    self.topics = topics;
                    self.subs = subs;
                    self.edit = None; // rebuild the settings editor from fresh values
                }
                Evt::Messages { key, dlq, msgs } => {
                    self.messages.insert((key, dlq), msgs);
                    self.selected_row = None;
                }
                Evt::Status(s) => {
                    self.log.push((now_hms_utc(), s.clone(), false));
                    self.status = s;
                    self.status_is_error = false;
                }
                Evt::Error(e) => {
                    self.log.push((now_hms_utc(), e.clone(), true));
                    self.status = e;
                    self.status_is_error = true;
                }
                Evt::Busy(b) => self.busy += if b { 1 } else { -1 },
            }
        }
    }
}

fn apply_style(ctx: &egui::Context) {
    // Segoe UI for text if available (always is on Windows); egui's fonts stay as fallback for icons
    let mut fonts = egui::FontDefinitions::default();
    let windir = std::env::var("WINDIR").unwrap_or_else(|_| "C:\\Windows".into());
    if let Ok(bytes) = std::fs::read(format!("{windir}\\Fonts\\segoeui.ttf")) {
        fonts.font_data.insert(
            "segoe".to_owned(),
            std::sync::Arc::new(egui::FontData::from_owned(bytes)),
        );
        fonts
            .families
            .get_mut(&egui::FontFamily::Proportional)
            .unwrap()
            .insert(0, "segoe".to_owned());
    }
    // serif family for headings (Claude style); falls back to proportional glyphs for icons
    let mut serif_list = fonts.families[&egui::FontFamily::Proportional].clone();
    if let Ok(bytes) = std::fs::read(format!("{windir}\\Fonts\\georgia.ttf")) {
        fonts.font_data.insert(
            "georgia".to_owned(),
            std::sync::Arc::new(egui::FontData::from_owned(bytes)),
        );
        serif_list.insert(0, "georgia".to_owned());
    }
    fonts.families.insert(serif(), serif_list);
    ctx.set_fonts(fonts);

    ctx.all_styles_mut(|s| {
        use egui::{FontFamily, FontId, TextStyle};
        s.text_styles.insert(TextStyle::Heading, FontId::new(21.0, FontFamily::Proportional));
        s.text_styles.insert(TextStyle::Body, FontId::new(15.0, FontFamily::Proportional));
        s.text_styles.insert(TextStyle::Button, FontId::new(14.5, FontFamily::Proportional));
        s.text_styles.insert(TextStyle::Small, FontId::new(12.0, FontFamily::Proportional));
        s.text_styles.insert(TextStyle::Monospace, FontId::new(13.5, FontFamily::Monospace));
        s.spacing.item_spacing = egui::vec2(8.0, 7.0);
        s.spacing.button_padding = egui::vec2(12.0, 6.0);
        s.spacing.interact_size.y = 30.0;

        let p = pal();
        let (faint, sel_stroke, btn_fill, btn_hover) = if p.dark {
            (
                Color32::from_rgb(0x2b, 0x2b, 0x29),
                Color32::from_rgb(0x55, 0x55, 0x52),
                Color32::from_rgb(0x2e, 0x2e, 0x2c),
                Color32::from_rgb(0x38, 0x38, 0x36),
            )
        } else {
            (
                Color32::from_rgb(0xf6, 0xf1, 0xe8),
                Color32::from_rgb(0xd2, 0xc8, 0xb6),
                Color32::from_rgb(0xf3, 0xed, 0xe2),
                Color32::from_rgb(0xec, 0xe4, 0xd6),
            )
        };
        let v = &mut s.visuals;
        *v = if p.dark { egui::Visuals::dark() } else { egui::Visuals::light() };
        v.panel_fill = p.panel;
        v.window_fill = p.card;
        v.window_stroke = egui::Stroke::new(1.0, p.card_border);
        v.window_corner_radius = egui::CornerRadius::same(16);
        v.window_shadow = card_shadow();
        v.extreme_bg_color = p.input_bg;
        v.faint_bg_color = faint;
        v.selection.bg_fill = p.selected_bg;
        v.selection.stroke = egui::Stroke::new(1.0, sel_stroke);
        v.hyperlink_color = p.accent;
        v.override_text_color = Some(p.text);

        let r = egui::CornerRadius::same(10);
        v.widgets.noninteractive.bg_stroke = egui::Stroke::new(1.0, p.card_border);
        v.widgets.noninteractive.corner_radius = r;
        v.widgets.inactive.weak_bg_fill = btn_fill;
        v.widgets.inactive.bg_stroke = egui::Stroke::new(1.0, p.card_border);
        v.widgets.inactive.corner_radius = r;
        v.widgets.hovered.weak_bg_fill = btn_hover;
        v.widgets.hovered.bg_stroke = egui::Stroke::new(1.0, sel_stroke);
        v.widgets.hovered.corner_radius = r;
        v.widgets.active.weak_bg_fill = p.selected_bg;
        v.widgets.active.corner_radius = r;
        v.widgets.open.corner_radius = r;
    });
}

/// Small painted tree icons (emoji glyphs render poorly in egui's fonts).
fn tree_icon(ui: &mut egui::Ui, kind: &str) {
    let (rect, _) = ui.allocate_exact_size(egui::vec2(16.0, 16.0), egui::Sense::hover());
    let p = ui.painter();
    let c = rect.center();
    let accent = pal().accent;
    let weak = pal().text_weak;
    match kind {
        "ns" => {
            for (i, (dx, dy)) in [(-3.5, -3.5), (3.5, -3.5), (-3.5, 3.5), (3.5, 3.5)].into_iter().enumerate() {
                let col = if i == 0 { accent } else { weak };
                p.rect_filled(
                    egui::Rect::from_center_size(c + egui::vec2(dx, dy), egui::vec2(5.5, 5.5)),
                    1.0,
                    col,
                );
            }
        }
        "queue" => {
            for (i, dy) in [-4.0, 0.0, 4.0].into_iter().enumerate() {
                let col = if i == 2 { accent } else { weak };
                p.rect_filled(
                    egui::Rect::from_center_size(c + egui::vec2(0.0, dy), egui::vec2(11.0, 2.6)),
                    1.3,
                    col,
                );
            }
        }
        "topic" => {
            p.circle_filled(c, 2.6, accent);
            p.circle_stroke(c, 6.0, egui::Stroke::new(1.6, weak));
        }
        _ => {
            p.rect_stroke(
                egui::Rect::from_center_size(c, egui::vec2(11.0, 9.0)),
                2.0,
                egui::Stroke::new(1.5, weak),
                egui::StrokeKind::Middle,
            );
            p.line_segment(
                [c + egui::vec2(-5.5, 1.5), c + egui::vec2(5.5, 1.5)],
                egui::Stroke::new(1.5, accent),
            );
        }
    }
}

fn tab_chip(ui: &mut egui::Ui, active: bool, label: &str, _color: Color32) -> bool {
    let text = if active {
        RichText::new(label).strong().color(Color32::WHITE)
    } else {
        RichText::new(label).color(pal().text_weak)
    };
    let btn = egui::Button::new(text)
        .fill(if active { pal().btn } else { Color32::TRANSPARENT })
        .corner_radius(egui::CornerRadius::same(16));
    ui.add(btn)
        .on_hover_cursor(egui::CursorIcon::PointingHand)
        .clicked()
}

fn pill(ui: &mut egui::Ui, text: String, fg: Color32, bg: Color32) {
    egui::Frame::NONE
        .fill(bg)
        .corner_radius(9)
        .inner_margin(egui::Margin::symmetric(7, 1))
        .show(ui, |ui| {
            ui.label(RichText::new(text).color(fg).size(11.5).strong());
        });
}

fn count_badge(ui: &mut egui::Ui, active: i64, dlq: i64) {
    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
        ui.add_space(14.0); // keep clear of the scrollbar
        if dlq > 0 {
            pill(ui, dlq.to_string(), pal().dlq_red, pal().red_dim);
        }
        if active > 0 {
            pill(ui, active.to_string(), pal().ok_green, pal().green_dim);
        } else if dlq == 0 {
            ui.label(RichText::new("–").color(pal().text_weak).small());
        }
    });
}

impl eframe::App for App {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.drain_events();
        if ui.ctx().input(|i| i.key_pressed(egui::Key::F5)) && self.connected_ns.is_some() {
            self.send(Cmd::Refresh);
        }

        self.top_bar(ui);
        self.status_bar(ui);
        self.tree_panel(ui);
        self.central(ui);
        let ctx = ui.ctx().clone();
        self.dialogs(&ctx);
    }
}

impl App {
    fn top_bar(&mut self, ui: &mut egui::Ui) {
        egui::Panel::top(egui::Id::new("top")).show(ui, |ui| {
            ui.add_space(2.0);
            egui::MenuBar::new().ui(ui, |ui| {
                ui.menu_button("File", |ui| {
                    if ui.button("Connect…").clicked() {
                        self.show_connect = true;
                    }
                    if ui.button("Disconnect").clicked() {
                        self.disconnect();
                    }
                    ui.separator();
                    if ui.button("Exit").clicked() {
                        ui.ctx().send_viewport_cmd(egui::ViewportCommand::Close);
                    }
                });
                ui.menu_button("Actions", |ui| {
                    if ui.button("Refresh    F5").clicked() && self.connected_ns.is_some() {
                        self.send(Cmd::Refresh);
                    }
                    ui.separator();
                    if ui.button("New queue…").clicked() {
                        self.new_entity = Some(("queue".into(), String::new(), String::new()));
                    }
                    if ui.button("New topic…").clicked() {
                        self.new_entity = Some(("topic".into(), String::new(), String::new()));
                    }
                });
                ui.menu_button("View", |ui| {
                    let dark = pal().dark;
                    if ui.selectable_label(!dark, "Light theme").clicked() && dark {
                        self.set_theme(false, &ui.ctx().clone());
                    }
                    if ui.selectable_label(dark, "Dark theme").clicked() && !dark {
                        self.set_theme(true, &ui.ctx().clone());
                    }
                    ui.separator();
                    if ui.button(if self.show_log { "Hide log panel" } else { "Show log panel" }).clicked() {
                        self.show_log = !self.show_log;
                    }
                });
                ui.menu_button("Help", |ui| {
                    if ui.button("About Service Bus Explorer Advance…").clicked() {
                        self.show_about = true;
                    }
                });
            });
            ui.add_space(5.0);
            ui.horizontal(|ui| {
                ui.add_space(4.0);
                ui.label(RichText::new("✳").color(pal().accent).size(20.0));
                ui.label(
                    RichText::new("Service Bus Explorer").family(serif()).size(19.0).color(pal().text),
                );
                ui.label(RichText::new("ADVANCE").color(pal().accent).size(11.0).strong());
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.add_space(4.0);
                    if ui
                        .add(
                            egui::Button::new(RichText::new("Connect…").strong().color(Color32::WHITE))
                                .fill(pal().btn)
                                .corner_radius(egui::CornerRadius::same(16)),
                        )
                        .clicked()
                    {
                        self.show_connect = true;
                    }
                    if self.connected_ns.is_some() && ui.button("⟳ Refresh (F5)").clicked() {
                        self.send(Cmd::Refresh);
                    }
                    if let Some(ns) = &self.connected_ns.clone() {
                        let short = ns.split('.').next().unwrap_or(ns);
                        ui.scope(|ui| {
                            pill(ui, format!("● {short}"), pal().ok_green, pal().green_dim);
                        })
                        .response
                        .on_hover_text(ns);
                    }
                    if self.busy > 0 {
                        ui.spinner();
                    }
                });
            });
            ui.add_space(7.0);
        });
    }

    fn status_bar(&mut self, ui: &mut egui::Ui) {
        egui::Panel::bottom(egui::Id::new("status")).show(ui, |ui| {
            if self.show_log {
                ui.add_space(4.0);
                egui::ScrollArea::vertical()
                    .id_salt("log")
                    .max_height(170.0)
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        for (t, msg, is_err) in &self.log {
                            ui.horizontal_wrapped(|ui| {
                                ui.label(RichText::new(format!("{t}")).color(pal().text_weak).monospace().small());
                                let color = if *is_err { pal().dlq_red } else { pal().text };
                                ui.label(RichText::new(msg).color(color).small());
                            });
                        }
                    });
                ui.separator();
            }
            ui.add_space(2.0);
            ui.horizontal(|ui| {
                let dot = if self.status_is_error { pal().dlq_red } else { pal().ok_green };
                ui.label(RichText::new("●").color(dot).small());
                let color = if self.status_is_error { pal().dlq_red } else { pal().text_weak };
                ui.label(RichText::new(&self.status).color(color));
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let label = if self.show_log { "Hide log" } else { "Log" };
                    if ui.small_button(label).clicked() {
                        self.show_log = !self.show_log;
                    }
                    if self.show_log && ui.small_button("Clear").clicked() {
                        self.log.clear();
                    }
                });
            });
            ui.add_space(2.0);
        });
    }

    fn tree_panel(&mut self, ui: &mut egui::Ui) {
        egui::Panel::left(egui::Id::new("tree"))
            .default_size(320.0)
            .size_range(220.0..=560.0)
            .show(ui, |ui| {
                ui.add_space(8.0);
                ui.add(
                    egui::TextEdit::singleline(&mut self.filter)
                        .hint_text("Filter entities")
                        .desired_width(f32::INFINITY),
                );
                ui.horizontal(|ui| {
                    if ui.button("+ New queue").clicked() {
                        self.new_entity = Some(("queue".into(), String::new(), String::new()));
                    }
                    if ui.button("+ New topic").clicked() {
                        self.new_entity = Some(("topic".into(), String::new(), String::new()));
                    }
                });
                ui.add_space(6.0);
                let filter = self.filter.to_lowercase();
                egui::ScrollArea::vertical().show(ui, |ui| {
                    if let Some(ns) = &self.connected_ns.clone() {
                        ui.horizontal(|ui| {
                            tree_icon(ui, "ns");
                            ui.label(RichText::new(format!("sb://{ns}/")).size(13.0).strong());
                        });
                    }
                    ui.indent("nsroot", |ui| {
                    egui::CollapsingHeader::new(
                        RichText::new(format!("Queues · {}", self.queues.len()))
                            .size(12.0)
                            .color(pal().text_weak),
                    )
                    .default_open(true)
                    .show(ui, |ui| {
                            let names: Vec<(String, i64, i64)> = self
                                .queues
                                .iter()
                                .filter(|q| filter.is_empty() || q.name.to_lowercase().contains(&filter))
                                .map(|q| (q.name.clone(), q.active, q.dead_letter))
                                .collect();
                            for (name, active, dlq) in names {
                                ui.horizontal(|ui| {
                                    tree_icon(ui, "queue");
                                    let is_sel = self.sel == Sel::Queue(name.clone());
                                    if ui
                                        .selectable_label(is_sel, RichText::new(&name).size(13.0))
                                        .on_hover_cursor(egui::CursorIcon::PointingHand)
                                        .clicked()
                                    {
                                        self.sel = Sel::Queue(name.clone());
                                        self.tab = Tab::Overview;
                                        self.selected_row = None;
                                    }
                                    count_badge(ui, active, dlq);
                                });
                            }
                        });
                    ui.add_space(4.0);
                    egui::CollapsingHeader::new(
                        RichText::new(format!("Topics · {}", self.topics.len()))
                            .size(12.0)
                            .color(pal().text_weak),
                    )
                    .default_open(true)
                    .show(ui, |ui| {
                            let topics: Vec<String> = self
                                .topics
                                .iter()
                                .filter(|t| {
                                    filter.is_empty()
                                        || t.name.to_lowercase().contains(&filter)
                                        || self.subs.get(&t.name).is_some_and(|ss| {
                                            ss.iter().any(|s| s.name.to_lowercase().contains(&filter))
                                        })
                                })
                                .map(|t| t.name.clone())
                                .collect();
                            for tname in topics {
                                let subs: Vec<(String, i64, i64)> = self
                                    .subs
                                    .get(&tname)
                                    .map(|ss| {
                                        ss.iter()
                                            .map(|s| (s.name.clone(), s.active, s.dead_letter))
                                            .collect()
                                    })
                                    .unwrap_or_default();
                                egui::CollapsingHeader::new(
                                    RichText::new(format!("{tname} · {}", subs.len())).size(13.0),
                                )
                                    .id_salt(&tname)
                                    .show(ui, |ui| {
                                        ui.horizontal(|ui| {
                                            tree_icon(ui, "topic");
                                            let is_sel = self.sel == Sel::Topic(tname.clone());
                                            if ui
                                                .selectable_label(is_sel, RichText::new("(topic itself)").size(13.0))
                                                .on_hover_cursor(egui::CursorIcon::PointingHand)
                                                .clicked()
                                            {
                                                self.sel = Sel::Topic(tname.clone());
                                                self.tab = Tab::Overview;
                                            }
                                        });
                                        for (sname, active, dlq) in subs {
                                            ui.horizontal(|ui| {
                                                tree_icon(ui, "sub");
                                                let is_sel = self.sel
                                                    == Sel::Sub { topic: tname.clone(), name: sname.clone() };
                                                if ui
                                                    .selectable_label(is_sel, RichText::new(&sname).size(13.0))
                                                    .on_hover_cursor(egui::CursorIcon::PointingHand)
                                                    .clicked()
                                                {
                                                    self.sel = Sel::Sub {
                                                        topic: tname.clone(),
                                                        name: sname.clone(),
                                                    };
                                                    self.tab = Tab::Overview;
                                                    self.selected_row = None;
                                                }
                                                count_badge(ui, active, dlq);
                                            });
                                        }
                                    });
                            }
                        });
                    });
                });
            });
    }

    fn central(&mut self, ui: &mut egui::Ui) {
        let frame = egui::Frame::central_panel(ui.style())
            .fill(pal().bg)
            .inner_margin(egui::Margin { left: 20, right: 20, top: 16, bottom: 12 });
        egui::CentralPanel::default().frame(frame).show(ui, |ui| {
            if self.sel == Sel::None {
                ui.centered_and_justified(|ui| {
                    ui.vertical_centered(|ui| {
                        ui.add_space(ui.available_height() * 0.38);
                        ui.label(RichText::new("✳").color(pal().accent).size(46.0));
                        ui.label(
                            RichText::new("Select a queue or subscription on the left")
                                .family(serif())
                                .size(19.0)
                                .color(pal().text_weak),
                        );
                    });
                });
                return;
            }
            let (kind, title) = match &self.sel {
                Sel::Queue(q) => ("QUEUE", q.clone()),
                Sel::Topic(t) => ("TOPIC", t.clone()),
                Sel::Sub { topic, name } => ("SUBSCRIPTION", format!("{topic} / {name}")),
                Sel::None => unreachable!(),
            };
            ui.horizontal(|ui| {
                ui.label(RichText::new(title).strong().size(20.0).color(pal().text));
                pill(ui, kind.to_string(), pal().text_weak, pal().card);
            });
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                let is_topic = matches!(self.sel, Sel::Topic(_));
                for (t, label, color) in [
                    (Tab::Overview, "Overview", pal().accent),
                    (Tab::Messages, "Messages", pal().accent),
                    (Tab::DeadLetter, "Dead-letter", pal().dlq_red),
                    (Tab::Send, "Send", pal().accent),
                ] {
                    if is_topic && matches!(t, Tab::Messages | Tab::DeadLetter) {
                        continue;
                    }
                    if tab_chip(ui, self.tab == t, label, color) {
                        self.tab = t;
                    }
                }
            });
            ui.add_space(2.0);
            ui.separator();
            match self.tab {
                Tab::Overview => self.overview_tab(ui),
                Tab::Messages => self.messages_tab(ui, false),
                Tab::DeadLetter => self.messages_tab(ui, true),
                Tab::Send => self.send_tab(ui),
            }
        });
    }

    fn overview_tab(&mut self, ui: &mut egui::Ui) {
        let Some(ent) = self.selected_entity().cloned() else {
            ui.label("Entity not loaded — hit Refresh.");
            return;
        };
        ui.horizontal(|ui| {
            stat_card(ui, "Active", ent.active.to_string(), pal().ok_green);
            stat_card(ui, "Dead-letter", ent.dead_letter.to_string(), pal().dlq_red);
            stat_card(ui, "Scheduled", ent.scheduled.to_string(), pal().amber);
            stat_card(ui, "Size", human_bytes(ent.size_bytes), ui.visuals().text_color());
        });
        ui.add_space(6.0);
        ui.horizontal(|ui| {
            if let Sel::Topic(t) = &self.sel {
                if ui.button("+ New subscription").clicked() {
                    self.new_entity = Some(("subscription".into(), t.clone(), String::new()));
                }
            }
            let (path, label) = match &self.sel {
                Sel::Queue(q) => (q.clone(), format!("queue '{q}'")),
                Sel::Topic(t) => (t.clone(), format!("topic '{t}' (and all subscriptions)")),
                Sel::Sub { topic, name } => (
                    format!("{topic}/Subscriptions/{name}"),
                    format!("subscription '{topic}/{name}'"),
                ),
                Sel::None => return,
            };
            if ui.button(RichText::new("Delete entity…").color(pal().dlq_red)).clicked() {
                self.confirm = Some((format!("Delete {label}?"), Cmd::DeleteEntity(path)));
            }
        });
        ui.add_space(6.0);
        ui.separator();

        let (path, tag) = match &self.sel {
            Sel::Queue(q) => (q.clone(), "QueueDescription"),
            Sel::Topic(t) => (t.clone(), "TopicDescription"),
            Sel::Sub { topic, name } => {
                (format!("{topic}/Subscriptions/{name}"), "SubscriptionDescription")
            }
            Sel::None => return,
        };
        let editable = mgmt::editable_fields(tag);

        // (re)build the edit buffer when the selection changes or after a refresh
        if self.edit.as_ref().map(|(k, _)| k.as_str()) != Some(path.as_str()) {
            let fields = editable
                .iter()
                .map(|f| {
                    let cur = ent
                        .props
                        .iter()
                        .find(|(k, _)| k == f)
                        .map(|(_, v)| v.clone())
                        .unwrap_or_default();
                    ((*f).to_string(), cur)
                })
                .collect();
            self.edit = Some((path.clone(), fields));
        }
        let mut edit = self.edit.take().unwrap();
        let mut update_clicked = false;
        let mut discard_clicked = false;

        egui::ScrollArea::vertical().show(ui, |ui| {
            ui.label(RichText::new("SETTINGS").size(11.5).color(pal().text_weak));
            ui.add_space(2.0);
            egui::Grid::new("settings")
                .num_columns(2)
                .striped(true)
                .min_col_width(260.0)
                .show(ui, |ui| {
                    for (name, val) in edit.1.iter_mut() {
                        ui.label(RichText::new(name.as_str()).color(pal().text_weak));
                        if val == "true" || val == "false" {
                            let mut b = val == "true";
                            if ui.checkbox(&mut b, "").changed() {
                                *val = b.to_string();
                            }
                        } else if name == "Status" {
                            egui::ComboBox::from_id_salt(name.as_str())
                                .selected_text(val.as_str())
                                .show_ui(ui, |ui| {
                                    for s in ["Active", "Disabled", "SendDisabled", "ReceiveDisabled"] {
                                        ui.selectable_value(val, s.to_string(), s);
                                    }
                                });
                        } else {
                            ui.add(egui::TextEdit::singleline(val).desired_width(300.0));
                        }
                        ui.end_row();
                    }
                });
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                if ui
                    .add(
                        egui::Button::new(RichText::new("Update entity").strong().color(Color32::WHITE))
                            .fill(pal().btn)
                            .corner_radius(egui::CornerRadius::same(16)),
                    )
                    .clicked()
                {
                    update_clicked = true;
                }
                if ui.button("Discard changes").clicked() {
                    discard_clicked = true;
                }
                ui.label(
                    RichText::new("Durations are ISO 8601, e.g. PT30S, PT1M, P14D")
                        .color(pal().text_weak)
                        .small(),
                );
            });
            ui.add_space(10.0);
            ui.separator();
            ui.label(RichText::new("INFORMATION").size(11.5).color(pal().text_weak));
            ui.add_space(2.0);
            egui::Grid::new("props")
                .num_columns(2)
                .striped(true)
                .min_col_width(260.0)
                .show(ui, |ui| {
                    for (k, v) in &ent.props {
                        if editable.contains(&k.as_str()) {
                            continue;
                        }
                        ui.label(RichText::new(k).color(pal().text_weak));
                        ui.label(v);
                        ui.end_row();
                    }
                });
            ui.add_space(20.0);
        });

        if update_clicked {
            self.send(Cmd::UpdateEntity {
                path: path.clone(),
                tag: tag.to_string(),
                fields: edit.1.clone(),
            });
        }
        if discard_clicked {
            self.edit = None; // rebuilds from current values next frame
        } else {
            self.edit = Some(edit);
        }
    }

    fn messages_tab(&mut self, ui: &mut egui::Ui, dlq: bool) {
        let Some(target) = self.target() else {
            ui.label("Pick a queue or subscription to browse messages.");
            return;
        };
        let key = (target.key(), dlq);
        ui.horizontal(|ui| {
            if ui
                .add(
                    egui::Button::new(RichText::new("Peek").strong().color(Color32::WHITE))
                        .fill(pal().btn)
                        .corner_radius(egui::CornerRadius::same(16)),
                )
                .clicked()
            {
                self.send(Cmd::Peek { target: target.clone(), dlq, max: self.peek_max });
            }
            egui::ComboBox::from_id_salt("peek_max")
                .selected_text(format!("top {}", self.peek_max))
                .width(90.0)
                .show_ui(ui, |ui| {
                    for n in [10u32, 32, 100, 500] {
                        ui.selectable_value(&mut self.peek_max, n, format!("top {n}"));
                    }
                });
            ui.separator();
            if ui
                .button(RichText::new("Receive & delete").color(pal().dlq_red))
                .on_hover_text("Destructive: removes messages from the entity")
                .clicked()
            {
                self.confirm = Some((
                    format!("Receive & DELETE up to {} message(s) from {}{}?", self.peek_max, key.0, if dlq { " (DLQ)" } else { "" }),
                    Cmd::Receive { target: target.clone(), dlq, max: self.peek_max },
                ));
            }
            if ui
                .button(RichText::new("Purge all").color(pal().dlq_red))
                .clicked()
            {
                self.confirm = Some((
                    format!("Purge ALL messages from {}{}?", key.0, if dlq { " (DLQ)" } else { "" }),
                    Cmd::Purge { target: target.clone(), dlq },
                ));
            }
            if dlq {
                ui.separator();
                let msgs = self.messages.get(&key);
                let sel_msg = self
                    .selected_row
                    .and_then(|i| msgs.and_then(|m| m.get(i)).cloned());
                if ui
                    .add_enabled(sel_msg.is_some(), egui::Button::new("Resubmit selected (copy)"))
                    .on_hover_text("Sends a copy back to the entity; original stays in the DLQ")
                    .clicked()
                {
                    self.send(Cmd::Resubmit { target: target.clone(), msgs: vec![sel_msg.unwrap()] });
                }
                if let Some(msgs) = msgs {
                    if !msgs.is_empty() && ui.button("Resubmit all (copies)").clicked() {
                        self.send(Cmd::Resubmit { target: target.clone(), msgs: msgs.clone() });
                    }
                }
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                let loaded = self.messages.get(&key).map_or(0, |m| m.len());
                let in_entity = self
                    .selected_entity()
                    .map(|e| if dlq { e.dead_letter } else { e.active })
                    .unwrap_or(0);
                ui.label(
                    RichText::new(format!("{loaded} loaded · {in_entity} in entity"))
                        .color(pal().text_weak),
                );
            });
        });
        ui.add_space(4.0);

        let msgs = self.messages.get(&key).cloned().unwrap_or_default();
        if msgs.is_empty() {
            ui.add_space(12.0);
            ui.label(RichText::new("No messages loaded — hit Peek.").weak());
            return;
        }

        let table_height = ui.available_height() * 0.5;
        egui::ScrollArea::horizontal().id_salt("tbl_h").show(ui, |ui| {
            TableBuilder::new(ui)
                .striped(true)
                .sense(egui::Sense::click())
                .max_scroll_height(table_height)
                .column(Column::auto().at_least(60.0)) // seq
                .column(Column::initial(220.0).resizable(true)) // id
                .column(Column::initial(160.0).resizable(true)) // subject
                .column(Column::initial(180.0)) // enqueued
                .column(Column::auto().at_least(50.0)) // size
                .column(Column::auto().at_least(40.0)) // delivery
                .column(Column::remainder()) // dlq reason / content type
                .header(22.0, |mut h| {
                    for t in ["Seq", "Message ID", "Subject", "Enqueued (UTC)", "Size", "DC", if dlq { "DL reason" } else { "Content type" }] {
                        h.col(|ui| { ui.label(RichText::new(t).strong()); });
                    }
                })
                .body(|body| {
                    body.rows(26.0, msgs.len(), |mut row| {
                        let i = row.index();
                        let m = &msgs[i];
                        row.set_selected(self.selected_row == Some(i));
                        let cell = |ui: &mut egui::Ui, text: String| {
                            ui.add(egui::Label::new(text).truncate().selectable(false));
                        };
                        row.col(|ui| cell(ui, m.seq.to_string()));
                        row.col(|ui| cell(ui, m.id.clone()));
                        row.col(|ui| cell(ui, m.subject.clone()));
                        row.col(|ui| cell(ui, m.enqueued.chars().take(23).collect()));
                        row.col(|ui| cell(ui, human_bytes(m.size as i64)));
                        row.col(|ui| cell(ui, m.delivery_count.to_string()));
                        row.col(|ui| {
                            if dlq {
                                ui.add(
                                    egui::Label::new(RichText::new(&m.dl_reason).color(pal().dlq_red))
                                        .truncate()
                                        .selectable(false),
                                );
                            } else {
                                cell(ui, m.content_type.clone());
                            }
                        });
                        let resp = row.response().on_hover_cursor(egui::CursorIcon::PointingHand);
                        if resp.clicked() {
                            self.selected_row = Some(i);
                        }
                    });
                });
        });

        ui.separator();
        if let Some(m) = self.selected_row.and_then(|i| msgs.get(i)) {
            egui::ScrollArea::vertical().id_salt("detail").show(ui, |ui| {
                ui.horizontal(|ui| {
                    pill(ui, format!("Seq {}", m.seq), pal().text_weak, pal().card);
                    ui.label(RichText::new(&m.id).strong());
                    if !m.id.is_empty() && ui.small_button("Copy ID").clicked() {
                        ui.ctx().copy_text(m.id.clone());
                    }
                });
                let expires = if m.expires.starts_with("9999") { "never".to_string() } else { m.expires.clone() };
                ui.horizontal_wrapped(|ui| {
                    for (k, v) in [
                        ("Enqueued", &m.enqueued),
                        ("Expires", &expires),
                        ("Delivery count", &m.delivery_count.to_string()),
                        ("Content type", &m.content_type),
                        ("Session", &m.session),
                        ("Correlation", &m.correlation),
                        ("DL description", &m.dl_description),
                    ] {
                        if !v.is_empty() {
                            ui.label(RichText::new(format!("{k}:")).color(pal().text_weak));
                            ui.label(v);
                            ui.separator();
                        }
                    }
                });
                if !m.props.is_empty() {
                    ui.collapsing("Application properties", |ui| {
                        egui::Grid::new("msg_props").striped(true).show(ui, |ui| {
                            for (k, v) in &m.props {
                                ui.label(RichText::new(k).weak());
                                ui.label(v);
                                ui.end_row();
                            }
                        });
                    });
                }
                ui.horizontal(|ui| {
                    ui.label(RichText::new("Body").strong());
                    if ui.small_button("Copy").clicked() {
                        ui.ctx().copy_text(m.body.clone());
                    }
                    if ui.small_button("Load into Send tab").clicked() {
                        self.out.body = m.body.clone();
                        self.out.subject = m.subject.clone();
                        self.out.content_type = m.content_type.clone();
                        self.out.props = m.props.clone();
                        self.tab = Tab::Send;
                    }
                });
                let pretty = serde_json::from_str::<serde_json::Value>(&m.body)
                    .ok()
                    .and_then(|v| serde_json::to_string_pretty(&v).ok())
                    .unwrap_or_else(|| m.body.clone());
                ui.add(
                    egui::TextEdit::multiline(&mut pretty.as_str())
                        .code_editor()
                        .desired_width(f32::INFINITY),
                );
            });
        } else {
            ui.label(RichText::new("Click a row to inspect the message.").weak());
        }
    }

    fn send_tab(&mut self, ui: &mut egui::Ui) {
        let destination = match &self.sel {
            Sel::Queue(q) => q.clone(),
            Sel::Topic(t) => t.clone(),
            Sel::Sub { topic, .. } => topic.clone(),
            Sel::None => return,
        };
        ui.label(
            RichText::new(format!("Sending to: {destination}"))
                .color(pal().accent)
                .strong(),
        );
        ui.add_space(4.0);
        egui::ScrollArea::vertical().show(ui, |ui| {
            egui::Grid::new("send_form").num_columns(4).show(ui, |ui| {
                ui.label("Subject");
                ui.text_edit_singleline(&mut self.out.subject);
                ui.label("Content type");
                ui.text_edit_singleline(&mut self.out.content_type);
                ui.end_row();
                ui.label("Message ID");
                ui.text_edit_singleline(&mut self.out.message_id);
                ui.label("Correlation ID");
                ui.text_edit_singleline(&mut self.out.correlation_id);
                ui.end_row();
                ui.label("Session ID");
                ui.text_edit_singleline(&mut self.out.session_id);
                ui.label("TTL (seconds)");
                ui.text_edit_singleline(&mut self.out.ttl_secs);
                ui.end_row();
            });
            ui.add_space(6.0);
            ui.label(RichText::new("Custom properties").strong());
            let mut remove: Option<usize> = None;
            for (i, (k, v)) in self.out.props.iter_mut().enumerate() {
                ui.horizontal(|ui| {
                    ui.add(egui::TextEdit::singleline(k).hint_text("key").desired_width(200.0));
                    ui.add(egui::TextEdit::singleline(v).hint_text("value").desired_width(320.0));
                    if ui.small_button("✖").clicked() {
                        remove = Some(i);
                    }
                });
            }
            if let Some(i) = remove {
                self.out.props.remove(i);
            }
            if ui.button("+ Add property").clicked() {
                self.out.props.push((String::new(), String::new()));
            }
            ui.add_space(6.0);
            ui.label(RichText::new("Body").strong());
            ui.add(
                egui::TextEdit::multiline(&mut self.out.body)
                    .code_editor()
                    .desired_rows(12)
                    .desired_width(f32::INFINITY),
            );
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.label("Copies:");
                let mut count = self.out.count.max(1);
                ui.add(egui::DragValue::new(&mut count).range(1..=10_000));
                self.out.count = count;
                if ui
                    .add(
                        egui::Button::new(RichText::new("Send message").strong().color(Color32::WHITE))
                            .fill(pal().btn)
                            .corner_radius(egui::CornerRadius::same(16)),
                    )
                    .clicked()
                {
                    self.send(Cmd::Send { destination: destination.clone(), msg: self.out.clone() });
                }
            });
        });
    }

    fn dialogs(&mut self, ctx: &egui::Context) {
        // new entity dialog
        let mut close = false;
        let mut pending: Option<Cmd> = None;
        if let Some((kind, topic, name)) = &mut self.new_entity {
            let title = match kind.as_str() {
                "queue" => "New queue".to_string(),
                "topic" => "New topic".to_string(),
                _ => format!("New subscription on {topic}"),
            };
            let mut submit = false;
            egui::Window::new(title)
                .collapsible(false)
                .resizable(false)
                .anchor(Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    let r = ui.add(egui::TextEdit::singleline(name).hint_text("name"));
                    submit = r.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                    ui.horizontal(|ui| {
                        if ui.button("Create").clicked() {
                            submit = true;
                        }
                        if ui.button("Cancel").clicked() {
                            close = true;
                        }
                    });
                });
            if submit && !name.trim().is_empty() {
                let name = name.trim().to_string();
                pending = Some(match kind.as_str() {
                    "queue" => Cmd::CreateQueue(name),
                    "topic" => Cmd::CreateTopic(name),
                    _ => Cmd::CreateSubscription { topic: topic.clone(), name },
                });
                close = true;
            }
        }
        if let Some(cmd) = pending {
            self.send(cmd);
        }
        if close {
            self.new_entity = None;
        }

        // connect dialog
        if self.show_connect {
            let mut open = true;
            egui::Window::new("Connect to Service Bus")
                .collapsible(false)
                .resizable(false)
                .open(&mut open)
                .anchor(Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.add_space(4.0);
                    ui.label(RichText::new("NAMESPACE CONNECTION STRING").size(11.0).color(pal().text_weak));
                    ui.add(
                        egui::TextEdit::singleline(&mut self.conn_str)
                            .hint_text("Endpoint=sb://…;SharedAccessKeyName=…;SharedAccessKey=…")
                            .password(true)
                            .desired_width(500.0),
                    );
                    ui.label(
                        RichText::new("Use a Manage-level key (e.g. RootManageSharedAccessKey) to browse the whole namespace.")
                            .color(pal().text_weak)
                            .small(),
                    );
                    if !self.config.connections.is_empty() {
                        ui.add_space(8.0);
                        ui.label(RichText::new("RECENT").size(11.0).color(pal().text_weak));
                        let mut pick: Option<String> = None;
                        for c in &self.config.connections {
                            let label = c
                                .split(';')
                                .find_map(|p| p.strip_prefix("Endpoint=sb://"))
                                .unwrap_or("connection")
                                .trim_end_matches('/');
                            if ui
                                .selectable_label(false, label)
                                .on_hover_cursor(egui::CursorIcon::PointingHand)
                                .clicked()
                            {
                                pick = Some(c.clone());
                            }
                        }
                        if let Some(c) = pick {
                            self.conn_str = c;
                        }
                    }
                    ui.add_space(10.0);
                    ui.horizontal(|ui| {
                        if ui
                            .add(
                                egui::Button::new(RichText::new("Connect").strong().color(Color32::WHITE))
                                    .fill(pal().btn)
                                    .corner_radius(egui::CornerRadius::same(16)),
                            )
                            .clicked()
                            && !self.conn_str.trim().is_empty()
                        {
                            self.send(Cmd::Connect(self.conn_str.trim().to_string()));
                            self.show_connect = false;
                        }
                        if ui.button("Cancel").clicked() {
                            self.show_connect = false;
                        }
                    });
                    ui.add_space(4.0);
                });
            if !open {
                self.show_connect = false;
            }
        }

        // about dialog
        if self.show_about {
            let mut open = true;
            egui::Window::new("About")
                .collapsible(false)
                .resizable(false)
                .open(&mut open)
                .anchor(Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.vertical_centered(|ui| {
                        ui.add_space(4.0);
                        ui.label(RichText::new("✳").color(pal().accent).size(34.0));
                        ui.label(RichText::new("Service Bus Explorer Advance").family(serif()).size(19.0));
                        ui.label(RichText::new(format!("Version {}", env!("CARGO_PKG_VERSION"))).color(pal().text_weak));
                    });
                    ui.add_space(8.0);
                    ui.label("A fast, native explorer for Azure Service Bus, written in Rust.");
                    ui.add_space(6.0);
                    ui.label(RichText::new("FEATURES").size(11.0).color(pal().text_weak));
                    ui.label("• Browse queues, topics & subscriptions with live counts\n• Peek, send, receive, purge & resubmit messages\n• Dead-letter management\n• Create, update & delete entities\n• Operation log");
                    ui.add_space(6.0);
                    ui.label(RichText::new("PLANNED").size(11.0).color(pal().text_weak));
                    ui.label("• Session browsing\n• Subscription rules & filters\n• Scheduled message management\n• Message import / export\n• Entra ID sign-in");
                    ui.add_space(6.0);
                    ui.label(RichText::new("Built with egui + azservicebus · © 2026").color(pal().text_weak).small());
                });
            self.show_about = open;
        }

        // confirm dialog
        if self.confirm.is_some() {
            let mut do_it = false;
            let mut cancel = false;
            let label = self.confirm.as_ref().unwrap().0.clone();
            egui::Window::new("Confirm")
                .collapsible(false)
                .resizable(false)
                .anchor(Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label(&label);
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui.button(RichText::new("Yes, do it").color(pal().dlq_red)).clicked() {
                            do_it = true;
                        }
                        if ui.button("Cancel").clicked() {
                            cancel = true;
                        }
                    });
                });
            if do_it {
                if let Some((_, cmd)) = self.confirm.take() {
                    self.send(cmd);
                }
            } else if cancel {
                self.confirm = None;
            }
        }
    }
}

fn stat_card(ui: &mut egui::Ui, label: &str, value: String, color: Color32) {
    egui::Frame::NONE
        .fill(pal().card)
        .stroke(egui::Stroke::new(1.0, pal().card_border))
        .corner_radius(16)
        .shadow(card_shadow())
        .inner_margin(egui::Margin { left: 18, right: 18, top: 12, bottom: 12 })
        .show(ui, |ui| {
            ui.set_min_width(104.0);
            ui.vertical(|ui| {
                ui.label(RichText::new(label.to_uppercase()).color(pal().text_weak).size(10.5));
                ui.label(RichText::new(value).size(23.0).strong().color(color));
            });
        });
}

fn human_bytes(b: i64) -> String {
    match b {
        b if b >= 1_048_576 => format!("{:.1} MB", b as f64 / 1_048_576.0),
        b if b >= 1024 => format!("{:.1} KB", b as f64 / 1024.0),
        b => format!("{b} B"),
    }
}
