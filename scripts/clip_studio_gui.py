#!/usr/bin/env python
"""Desktop GUI for generating clip options from a YouTube URL."""

from __future__ import annotations

import json
import queue
import threading
import traceback
import webbrowser
from datetime import date, datetime
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from clip_dashboard import (
    ClipOption,
    DashboardConfig,
    DashboardResult,
    discover_creator_videos,
    generate_dashboard,
)
from youtube_tiktok_pipeline import VideoCandidate


APP_TITLE = "Clip Studio ES"


class ClipStudioApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("1180x780")
        self.root.minsize(980, 680)
        self.root.configure(bg="#0d1117")

        self.queue: queue.Queue[tuple[str, str]] = queue.Queue()
        self.generate_worker: threading.Thread | None = None
        self.discovery_worker: threading.Thread | None = None
        self.last_result: DashboardResult | None = None
        self.discovered_candidates: list[VideoCandidate] = []
        self.auto_run_after_discovery = False

        self.url_var = tk.StringVar()
        self.duration_var = tk.IntVar(value=60)
        self.options_var = tk.IntVar(value=6)
        self.stride_var = tk.IntVar(value=10)
        self.overlap_var = tk.DoubleVar(value=0.40)
        self.discovery_scan_var = tk.IntVar(value=8)
        self.discovery_limit_var = tk.IntVar(value=12)
        self.discovery_week_only_var = tk.BooleanVar(value=True)
        self.auto_open_dashboard_var = tk.BooleanVar(value=True)
        self.output_dir_var = tk.StringVar(value="output")
        self.work_dir_var = tk.StringVar(value="work")

        self._configure_style()
        self._build_layout()
        self._poll_queue()

    def _configure_style(self) -> None:
        style = ttk.Style()
        style.theme_use("clam")

        style.configure("App.TFrame", background="#0d1117")
        style.configure("Panel.TFrame", background="#131a24")
        style.configure("Card.TFrame", background="#161f2d")
        style.configure("Header.TLabel", background="#0d1117", foreground="#e9eef5", font=("Segoe UI Semibold", 22))
        style.configure("Sub.TLabel", background="#0d1117", foreground="#99a7bb", font=("Segoe UI", 10))
        style.configure("Label.TLabel", background="#161f2d", foreground="#dfe6ef", font=("Segoe UI", 10))
        style.configure("Treeview", background="#101722", foreground="#e6edf5", fieldbackground="#101722", rowheight=30)
        style.configure("Treeview.Heading", background="#273346", foreground="#f4f8ff", font=("Segoe UI Semibold", 10))
        style.map("Treeview", background=[("selected", "#2f5ea4")], foreground=[("selected", "#ffffff")])

    def _build_layout(self) -> None:
        root_frame = ttk.Frame(self.root, style="App.TFrame", padding=18)
        root_frame.pack(fill="both", expand=True)

        header = ttk.Frame(root_frame, style="App.TFrame")
        header.pack(fill="x", pady=(0, 10))
        ttk.Label(header, text="Clip Studio ES", style="Header.TLabel").pack(anchor="w")
        ttk.Label(
            header,
            text="Pega URL de YouTube, genera opciones y elige qué clip subir manualmente.",
            style="Sub.TLabel",
        ).pack(anchor="w")

        main = ttk.Frame(root_frame, style="App.TFrame")
        main.pack(fill="both", expand=True)

        left = ttk.Frame(main, style="Card.TFrame", padding=14)
        left.pack(side="left", fill="y", padx=(0, 12))
        left.configure(width=360)
        left.pack_propagate(False)

        right = ttk.Frame(main, style="Panel.TFrame", padding=12)
        right.pack(side="left", fill="both", expand=True)

        self._build_controls(left)
        self._build_results(right)

    def _build_controls(self, parent: ttk.Frame) -> None:
        ttk.Label(parent, text="URL de YouTube", style="Label.TLabel").pack(anchor="w", pady=(0, 4))
        self.url_entry = tk.Entry(
            parent,
            textvariable=self.url_var,
            bg="#0f1620",
            fg="#f2f6fb",
            insertbackground="#f2f6fb",
            relief="flat",
            highlightthickness=1,
            highlightbackground="#2d3a4f",
            highlightcolor="#4b8fe8",
            font=("Segoe UI", 11),
        )
        self.url_entry.pack(fill="x", ipady=8, pady=(0, 10))
        self.url_entry.bind("<Control-a>", self._select_all_text)

        ttk.Label(parent, text="Descubrir videos creadores (Espana)", style="Label.TLabel").pack(anchor="w", pady=(2, 4))
        discover_controls = ttk.Frame(parent, style="Card.TFrame")
        discover_controls.pack(fill="x")

        self.discovery_btn = tk.Button(
            discover_controls,
            text="Buscar virales",
            command=lambda: self.start_creator_discovery(auto_run=False),
            bg="#3f6898",
            fg="#f4f7fb",
            relief="flat",
            font=("Segoe UI Semibold", 10),
            cursor="hand2",
            pady=6,
            padx=8,
        )
        self.discovery_btn.pack(side="left")

        ttk.Label(discover_controls, text="Scan/canal", style="Label.TLabel").pack(side="left", padx=(8, 4))
        tk.Spinbox(
            discover_controls,
            from_=5,
            to=50,
            width=4,
            textvariable=self.discovery_scan_var,
            bg="#0f1620",
            fg="#f2f6fb",
            insertbackground="#f2f6fb",
            relief="flat",
            highlightthickness=1,
            highlightbackground="#2d3a4f",
            highlightcolor="#4b8fe8",
            font=("Segoe UI", 9),
        ).pack(side="left")

        ttk.Label(discover_controls, text="Limite", style="Label.TLabel").pack(side="left", padx=(8, 4))
        tk.Spinbox(
            discover_controls,
            from_=5,
            to=50,
            width=4,
            textvariable=self.discovery_limit_var,
            bg="#0f1620",
            fg="#f2f6fb",
            insertbackground="#f2f6fb",
            relief="flat",
            highlightthickness=1,
            highlightbackground="#2d3a4f",
            highlightcolor="#4b8fe8",
            font=("Segoe UI", 9),
        ).pack(side="left")

        tk.Checkbutton(
            parent,
            text="Solo videos de esta semana",
            variable=self.discovery_week_only_var,
            bg="#161f2d",
            fg="#dfe6ef",
            selectcolor="#0f1620",
            activebackground="#161f2d",
            activeforeground="#dfe6ef",
            font=("Segoe UI", 9),
            relief="flat",
            highlightthickness=0,
        ).pack(anchor="w", pady=(6, 4))

        list_frame = ttk.Frame(parent, style="Card.TFrame")
        list_frame.pack(fill="x")
        self.discovery_list = tk.Listbox(
            list_frame,
            height=7,
            bg="#0f1620",
            fg="#dce6f2",
            selectbackground="#2f5ea4",
            selectforeground="#ffffff",
            relief="flat",
            highlightthickness=1,
            highlightbackground="#2d3a4f",
            font=("Segoe UI", 9),
        )
        self.discovery_list.pack(side="left", fill="both", expand=True)
        self.discovery_list.bind("<Double-Button-1>", self.use_selected_discovery)
        scroll = tk.Scrollbar(list_frame, orient="vertical", command=self.discovery_list.yview)
        scroll.pack(side="left", fill="y")
        self.discovery_list.configure(yscrollcommand=scroll.set)

        tk.Button(
            parent,
            text="Usar video seleccionado",
            command=self.use_selected_discovery,
            bg="#334865",
            fg="#f4f7fb",
            relief="flat",
            font=("Segoe UI", 9),
            cursor="hand2",
            pady=6,
        ).pack(fill="x", pady=(6, 8))

        tk.Button(
            parent,
            text="Auto IA (buscar + generar)",
            command=lambda: self.start_creator_discovery(auto_run=True),
            bg="#4c7f3a",
            fg="#f4f7fb",
            relief="flat",
            font=("Segoe UI Semibold", 9),
            cursor="hand2",
            pady=7,
        ).pack(fill="x", pady=(0, 8))

        self._labeled_scale(parent, "Duración clip (seg)", self.duration_var, 20, 90)
        self._labeled_spin(parent, "Número de opciones", self.options_var, 2, 12)
        self._labeled_spin(parent, "Stride (seg)", self.stride_var, 5, 30)

        ttk.Label(parent, text="Overlap máximo (0.10 - 0.80)", style="Label.TLabel").pack(anchor="w", pady=(14, 4))
        overlap_entry = tk.Entry(
            parent,
            textvariable=self.overlap_var,
            bg="#0f1620",
            fg="#f2f6fb",
            insertbackground="#f2f6fb",
            relief="flat",
            highlightthickness=1,
            highlightbackground="#2d3a4f",
            highlightcolor="#4b8fe8",
            font=("Segoe UI", 11),
        )
        overlap_entry.pack(fill="x", ipady=6, pady=(0, 10))

        self._path_picker(parent, "Output dir", self.output_dir_var)
        self._path_picker(parent, "Work dir", self.work_dir_var)

        actions = ttk.Frame(parent, style="Card.TFrame")
        actions.pack(fill="x", pady=(12, 10))

        self.generate_btn = tk.Button(
            actions,
            text="Generar Opciones",
            command=self.start_generation,
            bg="#ff7a2f",
            fg="#111111",
            activebackground="#ff985e",
            activeforeground="#111111",
            relief="flat",
            font=("Segoe UI Semibold", 11),
            cursor="hand2",
            padx=12,
            pady=10,
        )
        self.generate_btn.pack(fill="x")

        tk.Checkbutton(
            parent,
            text="Abrir dashboard automaticamente al terminar",
            variable=self.auto_open_dashboard_var,
            bg="#161f2d",
            fg="#dfe6ef",
            selectcolor="#0f1620",
            activebackground="#161f2d",
            activeforeground="#dfe6ef",
            font=("Segoe UI", 9),
            relief="flat",
            highlightthickness=0,
        ).pack(anchor="w", pady=(8, 2))

        self.progress = ttk.Progressbar(parent, mode="indeterminate")
        self.progress.pack(fill="x", pady=(10, 8))

        quick = ttk.Frame(parent, style="Card.TFrame")
        quick.pack(fill="x")
        tk.Button(
            quick,
            text="Abrir dashboard",
            command=self.open_dashboard,
            bg="#334865",
            fg="#f4f7fb",
            relief="flat",
            font=("Segoe UI", 10),
            cursor="hand2",
            pady=8,
        ).pack(fill="x", pady=(0, 6))
        tk.Button(
            quick,
            text="Abrir carpeta output",
            command=self.open_output_dir,
            bg="#243448",
            fg="#f4f7fb",
            relief="flat",
            font=("Segoe UI", 10),
            cursor="hand2",
            pady=8,
        ).pack(fill="x")

    def _build_results(self, parent: ttk.Frame) -> None:
        top = ttk.Frame(parent, style="Panel.TFrame")
        top.pack(fill="both", expand=True)

        cols = ("option", "start", "end", "score", "interest", "reach", "audio", "visual", "desc", "file")
        self.tree = ttk.Treeview(top, columns=cols, show="headings", height=14)
        for c, w in [
            ("option", 70),
            ("start", 70),
            ("end", 70),
            ("score", 80),
            ("interest", 80),
            ("reach", 80),
            ("audio", 70),
            ("visual", 70),
            ("desc", 270),
            ("file", 220),
        ]:
            self.tree.heading(c, text=c.upper())
            self.tree.column(c, width=w, anchor="w")
        self.tree.pack(fill="both", expand=True)
        self.tree.bind("<Double-1>", self.open_selected_file)

        bottom = ttk.Frame(parent, style="Panel.TFrame")
        bottom.pack(fill="both", expand=True, pady=(12, 0))
        ttk.Label(bottom, text="Log", style="Label.TLabel").pack(anchor="w", pady=(0, 6))
        self.log_box = tk.Text(
            bottom,
            height=12,
            bg="#0f1620",
            fg="#dce6f2",
            insertbackground="#dce6f2",
            relief="flat",
            highlightthickness=1,
            highlightbackground="#2d3a4f",
            font=("Consolas", 10),
            wrap="word",
        )
        self.log_box.pack(fill="both", expand=True)

    def _labeled_spin(self, parent: ttk.Frame, label: str, var: tk.IntVar, min_v: int, max_v: int) -> None:
        ttk.Label(parent, text=label, style="Label.TLabel").pack(anchor="w", pady=(10, 4))
        spin = tk.Spinbox(
            parent,
            from_=min_v,
            to=max_v,
            textvariable=var,
            bg="#0f1620",
            fg="#f2f6fb",
            insertbackground="#f2f6fb",
            relief="flat",
            highlightthickness=1,
            highlightbackground="#2d3a4f",
            highlightcolor="#4b8fe8",
            font=("Segoe UI", 11),
        )
        spin.pack(fill="x", ipady=6)

    def _labeled_scale(self, parent: ttk.Frame, label: str, var: tk.IntVar, min_v: int, max_v: int) -> None:
        ttk.Label(parent, text=f"{label}: {var.get()}", style="Label.TLabel").pack(anchor="w", pady=(10, 4))
        lbl = parent.winfo_children()[-1]

        def on_change(value: str) -> None:
            var.set(int(float(value)))
            lbl.configure(text=f"{label}: {var.get()}")

        scale = tk.Scale(
            parent,
            from_=min_v,
            to=max_v,
            orient="horizontal",
            variable=var,
            command=on_change,
            bg="#161f2d",
            fg="#dfe6ef",
            troughcolor="#0f1620",
            highlightthickness=0,
            activebackground="#ff8b49",
        )
        scale.pack(fill="x")

    def _path_picker(self, parent: ttk.Frame, label: str, var: tk.StringVar) -> None:
        ttk.Label(parent, text=label, style="Label.TLabel").pack(anchor="w", pady=(10, 4))
        row = ttk.Frame(parent, style="Card.TFrame")
        row.pack(fill="x")
        entry = tk.Entry(
            row,
            textvariable=var,
            bg="#0f1620",
            fg="#f2f6fb",
            insertbackground="#f2f6fb",
            relief="flat",
            highlightthickness=1,
            highlightbackground="#2d3a4f",
            highlightcolor="#4b8fe8",
            font=("Segoe UI", 10),
        )
        entry.pack(side="left", fill="x", expand=True, ipady=6)
        tk.Button(
            row,
            text="...",
            command=lambda: self.pick_dir(var),
            bg="#2d3f57",
            fg="#f2f6fb",
            relief="flat",
            width=4,
            cursor="hand2",
        ).pack(side="left", padx=(6, 0))

    def pick_dir(self, variable: tk.StringVar) -> None:
        selected = filedialog.askdirectory()
        if selected:
            variable.set(selected)

    def start_creator_discovery(self, auto_run: bool = False) -> None:
        if self.discovery_worker and self.discovery_worker.is_alive():
            return

        self.auto_run_after_discovery = auto_run
        per_channel_scan = int(self.discovery_scan_var.get())
        max_results = int(self.discovery_limit_var.get())
        this_week_only = bool(self.discovery_week_only_var.get())

        self._log("Buscando videos virales de creadores...")
        self.discovery_list.delete(0, "end")
        self.discovery_btn.configure(state="disabled")

        def worker() -> None:
            try:
                candidates = discover_creator_videos(
                    per_channel_scan=per_channel_scan,
                    this_week_only=this_week_only,
                    max_results=max_results,
                    log_fn=lambda m: self.queue.put(("log", f"[discovery] {m}")),
                )
                payload = json.dumps(
                    [
                        {
                            "title": c.title,
                            "url": c.url,
                            "view_count": c.view_count,
                            "duration": c.duration,
                            "channel": c.channel,
                            "video_id": c.video_id,
                            "upload_date": c.upload_date,
                            "views_per_day": c.views_per_day,
                            "ai_score": c.ai_score,
                            "ai_reason": c.ai_reason,
                        }
                        for c in candidates
                    ],
                    ensure_ascii=False,
                )
                self.queue.put(("discover_done", payload))
            except Exception as exc:
                tb = traceback.format_exc(limit=2)
                self.queue.put(("discover_error", f"{exc}\n{tb}"))

        self.discovery_worker = threading.Thread(target=worker, daemon=True)
        self.discovery_worker.start()

    def start_generation(self) -> None:
        if self.generate_worker and self.generate_worker.is_alive():
            return

        url = self.url_var.get().strip()
        if not url or "youtube.com" not in url and "youtu.be" not in url:
            messagebox.showerror(APP_TITLE, "Pega una URL valida de YouTube.")
            return

        self.tree.delete(*self.tree.get_children())
        self.last_result = None
        self._log("Iniciando generacion...")
        self.generate_btn.configure(state="disabled")
        self.progress.start(10)

        config = DashboardConfig(
            url=url,
            duration=int(self.duration_var.get()),
            options=int(self.options_var.get()),
            stride=int(self.stride_var.get()),
            overlap_ratio=float(self.overlap_var.get()),
            output_dir=self.output_dir_var.get().strip() or "output",
            work_dir=self.work_dir_var.get().strip() or "work",
        )

        def worker() -> None:
            try:
                result = generate_dashboard(config, log_fn=lambda m: self.queue.put(("log", m)))
                self.queue.put(("done", json.dumps(
                    {
                        "dashboard_dir": result.dashboard_dir,
                        "dashboard_html": result.dashboard_html,
                        "manifest_path": result.manifest_path,
                        "source_title": result.source_title,
                        "source_url": result.source_url,
                        "options": [o.__dict__ for o in result.options],
                    },
                    ensure_ascii=False,
                )))
            except Exception as exc:
                tb = traceback.format_exc(limit=2)
                self.queue.put(("error", f"{exc}\n{tb}"))

        self.generate_worker = threading.Thread(target=worker, daemon=True)
        self.generate_worker.start()

    def _poll_queue(self) -> None:
        try:
            while True:
                kind, payload = self.queue.get_nowait()
                if kind == "log":
                    self._log(payload)
                elif kind == "error":
                    self._log("ERROR: " + payload)
                    self.progress.stop()
                    self.generate_btn.configure(state="normal")
                    messagebox.showerror(APP_TITLE, "Error generando clips. Revisa el log.")
                elif kind == "done":
                    data = json.loads(payload)
                    options = data.pop("options")
                    result = DashboardResult(
                        options=[],
                        **data,
                    )
                    for o in options:
                        result.options.append(ClipOption(**o))
                    self.last_result = result
                    self._render_options(options)
                    self._log(f"Listo. Dashboard: {result.dashboard_html}")
                    self._log(
                        "Ranking: SCORE=46% INTEREST + 33% REACH + 12% AUDIO + 9% VISUAL, "
                        "con penalizacion por clips tematicamente repetidos."
                    )
                    self.progress.stop()
                    self.generate_btn.configure(state="normal")
                    messagebox.showinfo(APP_TITLE, "Opciones generadas.")
                    if self.auto_open_dashboard_var.get():
                        self.open_dashboard()
                elif kind == "discover_done":
                    self._on_discovery_done(payload)
                elif kind == "discover_error":
                    self._log("ERROR discovery: " + payload)
                    self.discovery_btn.configure(state="normal")
                    messagebox.showerror(APP_TITLE, "Error buscando videos de creadores.")
        except queue.Empty:
            pass
        finally:
            self.root.after(120, self._poll_queue)

    def _on_discovery_done(self, payload: str) -> None:
        raw = json.loads(payload)
        self.discovered_candidates = [
            VideoCandidate(
                title=item["title"],
                url=item["url"],
                view_count=int(item.get("view_count") or 0),
                duration=item.get("duration"),
                channel=item.get("channel") or "",
                video_id=item.get("video_id") or "",
                upload_date=item.get("upload_date"),
                views_per_day=float(item.get("views_per_day") or 0.0),
                ai_score=float(item.get("ai_score") or 0.0),
                ai_reason=item.get("ai_reason") or "",
            )
            for item in raw
        ]
        self.discovery_list.delete(0, "end")
        for idx, c in enumerate(self.discovered_candidates, start=1):
            views = self._fmt_views(c.view_count)
            vpd = self._fmt_views(int(c.views_per_day))
            age = self._age_label(c.upload_date)
            title = c.title.strip().replace("\n", " ")
            if len(title) > 62:
                title = title[:59] + "..."
            line = f"{idx:02}. AI {c.ai_score:.1f} | {c.channel} | {views} ({vpd}/dia) | {age} | {title}"
            self.discovery_list.insert("end", line)
        self._log(f"Descubrimiento completo: {len(self.discovered_candidates)} videos.")
        self.discovery_btn.configure(state="normal")
        if self.discovered_candidates:
            self.discovery_list.selection_set(0)
            self.discovery_list.activate(0)
            best = self.discovered_candidates[0]
            self._log(f"Top IA: {best.title} | score={best.ai_score:.1f} | {best.ai_reason}")
            if self.auto_run_after_discovery:
                self.url_var.set(best.url)
                self._log("Auto IA activo: iniciando generacion con el mejor candidato.")
                self.auto_run_after_discovery = False
                self.start_generation()
        else:
            self._log("No hubo resultados. Prueba desmarcar 'Solo videos de esta semana' o subir Scan/canal.")
            self.auto_run_after_discovery = False

    def use_selected_discovery(self, _event=None) -> None:
        selection = self.discovery_list.curselection()
        if not selection:
            return
        idx = selection[0]
        if idx < 0 or idx >= len(self.discovered_candidates):
            return
        cand = self.discovered_candidates[idx]
        self.url_var.set(cand.url)
        self.url_entry.focus_set()
        self._log(f"URL seleccionada: {cand.title} | AI {cand.ai_score:.1f} | {cand.ai_reason}")

    @staticmethod
    def _fmt_views(value: int) -> str:
        if value >= 1_000_000:
            return f"{value / 1_000_000:.1f}M"
        if value >= 1_000:
            return f"{value / 1_000:.1f}K"
        return str(value)

    @staticmethod
    def _age_label(upload_date: str | None) -> str:
        if not upload_date:
            return "fecha N/A"
        try:
            published = datetime.strptime(upload_date, "%Y%m%d").date()
        except Exception:
            return "fecha N/A"
        days = max(0, (date.today() - published).days)
        if days == 0:
            return "hoy"
        if days == 1:
            return "1 dia"
        return f"{days} dias"

    @staticmethod
    def _select_all_text(event) -> str:
        event.widget.select_range(0, "end")
        event.widget.icursor("end")
        return "break"

    def _render_options(self, options: list[dict]) -> None:
        self.tree.delete(*self.tree.get_children())
        for opt in options:
            self.tree.insert(
                "",
                "end",
                values=(
                    opt["option_id"],
                    f'{opt["start"]:.1f}',
                    f'{opt["end"]:.1f}',
                    f'{opt["score"]:.1f}',
                    f'{opt["interest_score"]:.1f}',
                    f'{opt["reach_score"]:.1f}',
                    f'{opt["audio_score"]:.1f}',
                    f'{opt["visual_score"]:.1f}',
                    opt["short_description"],
                    Path(opt["manual_upload_file"]).name,
                ),
                tags=(opt["manual_upload_file"],),
            )

    def open_selected_file(self, _event=None) -> None:
        sel = self.tree.selection()
        if not sel:
            return
        tags = self.tree.item(sel[0], "tags")
        if not tags:
            return
        file_path = Path(tags[0])
        if file_path.exists():
            webbrowser.open(file_path.resolve().as_uri())

    def open_dashboard(self) -> None:
        if not self.last_result:
            messagebox.showwarning(APP_TITLE, "Primero genera opciones.")
            return
        webbrowser.open(Path(self.last_result.dashboard_html).resolve().as_uri())

    def open_output_dir(self) -> None:
        out = Path(self.output_dir_var.get().strip() or "output")
        out.mkdir(parents=True, exist_ok=True)
        webbrowser.open(out.resolve().as_uri())

    def _log(self, message: str) -> None:
        self.log_box.insert("end", f"{message}\n")
        self.log_box.see("end")


def main() -> int:
    root = tk.Tk()
    app = ClipStudioApp(root)
    app._log("Pulsa 'Buscar virales' o pega URL y pulsa Generar Opciones.")
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
