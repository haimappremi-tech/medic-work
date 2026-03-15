#!/usr/bin/env python3
"""
file_ui.py
----------
Tkinter GUI for reviewing and resolving duplicate files.

Streaming review (new)
----------------------
Results appear in a live inbox as soon as they are found — you don't have
to wait for the scan to finish.  Each duplicate group gets an inbox row;
click "Review" to open it.  The scan progress bar keeps updating in the
background while you work through earlier results.

Flow
----
  Landing  →  Scan (live inbox)  →  Review group  →  back to inbox  →  …
          →  Finish & copy  (available once all groups are decided)

Usage
-----
    python3 file_ui.py                    # folder-picker dialog
    python3 file_ui.py <folder>           # scan immediately
    python3 file_ui.py <folder> 0.90      # custom threshold

Requirements
------------
    pip install PyMuPDF Pillow
"""

import os
import queue
import shutil
import sys
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from pathlib import Path

from check_similar_files import scan_files_streaming

try:
    import fitz
    from PIL import Image, ImageTk
    PREVIEW_OK = True
except ImportError:
    PREVIEW_OK = False


# ── palette ───────────────────────────────────────────────────────────────────
BG         = "#F7F6F2"
SURFACE    = "#FFFFFF"
BORDER     = "#E2E0D8"
TEXT       = "#1C1C1A"
TEXT_MUTED = "#72706A"
ACCENT     = "#185FA5"
ACCENT_LT  = "#E6F1FB"
DANGER     = "#A32D2D"
DANGER_LT  = "#FCEBEB"
SUCCESS    = "#3B6D11"
SUCCESS_LT = "#EAF3DE"
AMBER      = "#854F0B"
AMBER_LT   = "#FAEEDA"
GRAY_TILE  = "#D3D1C7"
GRAY_ICON  = "#888780"

FONT_SMALL = ("Georgia", 9)
FONT_BODY  = ("Georgia", 11)
FONT_BOLD  = ("Georgia", 11, "bold")
FONT_TITLE = ("Georgia", 14, "bold")
FONT_HEAD  = ("Georgia", 16, "bold")

_SPINNER = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]


# ── helpers ───────────────────────────────────────────────────────────────────

def _label(parent, text, font=FONT_BODY, color=TEXT, bg=None, **kw):
    bg = bg if bg is not None else parent["bg"]
    return tk.Label(parent, text=text, font=font, fg=color, bg=bg, anchor="w", **kw)

def _btn(parent, text, command, bg=SURFACE, fg=TEXT, bold=False, width=None):
    f = ("Georgia", 10, "bold") if bold else ("Georgia", 10)
    kw = {"width": width} if width else {}
    return tk.Button(
        parent, text=text, command=command, font=f, fg=fg, bg=bg,
        activebackground=bg, activeforeground=fg,
        relief="flat", bd=0, padx=14, pady=7, cursor="hand2",
        highlightbackground=BORDER, highlightthickness=1, **kw,
    )

def _badge(parent, text, bg, fg):
    return tk.Label(parent, text=text, font=FONT_SMALL, fg=fg, bg=bg, padx=8, pady=3)

def _render_pdf(path: str, width: int = 320):
    if not PREVIEW_OK or not path.lower().endswith(".pdf"):
        return None
    try:
        doc  = fitz.open(path)
        page = doc[0]
        mat  = fitz.Matrix(width / page.rect.width, width / page.rect.width)
        pix  = page.get_pixmap(matrix=mat, alpha=False)
        img  = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        doc.close()
        return ImageTk.PhotoImage(img)
    except Exception:
        return None

def _open_file(path: str):
    import subprocess
    if sys.platform == "win32":
        os.startfile(path)
    elif sys.platform == "darwin":
        subprocess.Popen(["open", path])
    else:
        subprocess.Popen(["xdg-open", path])


# ── main app ──────────────────────────────────────────────────────────────────

class FileUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Duplicate File Finder")
        self.geometry("1100x760")
        self.minsize(860, 580)
        self.configure(bg=BG)
        self._photos      = []
        self._spin_job    = None
        self._spin_idx    = 0
        self._show_landing()

    # ── landing ───────────────────────────────────────────────────────────────

    def _show_landing(self):
        self._clear()
        wrap = tk.Frame(self, bg=BG)
        wrap.place(relx=0.5, rely=0.45, anchor="center")

        _label(wrap, "Duplicate File Finder", font=FONT_HEAD).pack(pady=(0, 4))
        _label(wrap, "Scan a folder for exact and near-duplicate files.",
               color=TEXT_MUTED).pack()
        tk.Frame(wrap, bg=BORDER, height=1).pack(fill="x", pady=22)

        _label(wrap, "Similarity threshold", font=FONT_SMALL, color=TEXT_MUTED).pack(anchor="w")
        self._thresh = tk.DoubleVar(value=0.85)
        row = tk.Frame(wrap, bg=BG)
        row.pack(fill="x", pady=(4, 22))
        ttk.Scale(row, from_=0.5, to=1.0, variable=self._thresh,
                  orient="horizontal", length=280).pack(side="left")
        pct = _label(row, "85%", font=FONT_SMALL, color=TEXT_MUTED)
        pct.pack(side="left", padx=10)
        self._thresh.trace_add("write", lambda *_: pct.config(text=f"{self._thresh.get():.0%}"))

        _btn(wrap, "  Choose folder & scan  →",
             self._pick_and_scan, bg=ACCENT, fg=SURFACE, bold=True).pack(pady=4, ipadx=6)

    def _pick_and_scan(self):
        folder = filedialog.askdirectory(title="Select folder to scan")
        if folder:
            self._start_scan(folder, self._thresh.get())

    # ── scan + live inbox ─────────────────────────────────────────────────────

    def _start_scan(self, folder: str, threshold: float):
        self._clear()
        self._folder    = folder
        self._threshold = threshold
        self._groups    = []          # list of group dicts added as found
        self._decisions = {}          # path -> bool
        self._all_files = []
        self._scan_done = False
        self._event_q   = queue.Queue()

        # ── top bar ───────────────────────────────────────────────────────────
        bar = tk.Frame(self, bg=SURFACE, highlightbackground=BORDER, highlightthickness=1)
        bar.pack(fill="x", side="top")
        bar_inner = tk.Frame(bar, bg=SURFACE)
        bar_inner.pack(fill="x", padx=16, pady=10)
        _label(bar_inner, "Duplicate File Finder", font=FONT_TITLE).pack(side="left")
        _btn(bar_inner, "← Restart", self._show_landing).pack(side="right")

        # ── progress strip ────────────────────────────────────────────────────
        prog_frame = tk.Frame(self, bg=ACCENT_LT,
                              highlightbackground=BORDER, highlightthickness=1)
        prog_frame.pack(fill="x", side="top")
        prog_inner = tk.Frame(prog_frame, bg=ACCENT_LT)
        prog_inner.pack(fill="x", padx=14, pady=8)

        self._spin_lbl = _label(prog_inner, _SPINNER[0],
                                font=("Georgia", 14), color=ACCENT, bg=ACCENT_LT)
        self._spin_lbl.pack(side="left", padx=(0, 8))
        self._phase_lbl = _label(prog_inner, "Starting…",
                                 font=FONT_SMALL, color=ACCENT, bg=ACCENT_LT)
        self._phase_lbl.pack(side="left")
        self._pct_lbl = _label(prog_inner, "",
                               font=FONT_SMALL, color=ACCENT, bg=ACCENT_LT)
        self._pct_lbl.pack(side="left", padx=(8, 0))

        # progress bar
        bar_bg = tk.Frame(prog_inner, bg=BORDER, height=6, width=300)
        bar_bg.pack(side="right", padx=(0, 4))
        bar_bg.pack_propagate(False)
        self._prog_fill = tk.Frame(bar_bg, bg=ACCENT, width=0, height=6)
        self._prog_fill.place(x=0, y=0)
        self._prog_bar_width = 300

        # ── inbox hint ────────────────────────────────────────────────────────
        hint = tk.Frame(self, bg=AMBER_LT, highlightbackground=BORDER, highlightthickness=1)
        hint.pack(fill="x", side="top")
        _label(hint,
               "Duplicate groups appear below as they are found — click Review to decide.",
               font=FONT_SMALL, color=AMBER, bg=AMBER_LT).pack(side="left", padx=14, pady=7)

        # ── scrollable inbox ──────────────────────────────────────────────────
        inbox_outer = tk.Frame(self, bg=BG)
        inbox_outer.pack(fill="both", expand=True, padx=0, pady=0)

        canvas = tk.Canvas(inbox_outer, bg=BG, highlightthickness=0)
        vsb    = ttk.Scrollbar(inbox_outer, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        self._inbox_frame = tk.Frame(canvas, bg=BG)
        self._inbox_win   = canvas.create_window((0, 0), window=self._inbox_frame, anchor="nw")

        def _on_frame_resize(e):
            canvas.configure(scrollregion=canvas.bbox("all"))
        def _on_canvas_resize(e):
            canvas.itemconfig(self._inbox_win, width=e.width)

        self._inbox_frame.bind("<Configure>", _on_frame_resize)
        canvas.bind("<Configure>", _on_canvas_resize)
        canvas.bind_all("<MouseWheel>",
                        lambda e: canvas.yview_scroll(int(-1*(e.delta/120)), "units"))

        self._empty_lbl = _label(self._inbox_frame,
                                 "Waiting for first result…",
                                 color=TEXT_MUTED, font=FONT_SMALL)
        self._empty_lbl.pack(pady=20, padx=20)

        # ── bottom action bar ─────────────────────────────────────────────────
        self._act_bar = tk.Frame(self, bg=SURFACE,
                                 highlightbackground=BORDER, highlightthickness=1)
        self._act_bar.pack(fill="x", side="bottom")
        act_inner = tk.Frame(self._act_bar, bg=SURFACE)
        act_inner.pack(fill="x", padx=20, pady=12)

        self._finish_btn = _btn(act_inner, "Finish & copy →",
                                self._copy_output, bg=ACCENT, fg=SURFACE, bold=True)
        self._finish_btn.pack(side="right")
        self._finish_btn.config(state="disabled")

        self._status_lbl = _label(act_inner, "Scan in progress…",
                                  font=FONT_SMALL, color=TEXT_MUTED)
        self._status_lbl.pack(side="left")

        # ── start background scan ─────────────────────────────────────────────
        def _worker():
            try:
                for event in scan_files_streaming(folder, threshold):
                    self._event_q.put(event)
            except Exception as exc:
                self._event_q.put({"kind": "error", "msg": str(exc)})

        threading.Thread(target=_worker, daemon=True).start()
        self._spin_job = self.after(80, self._spin_tick)
        self.after(50, self._drain_queue)

    # ── spinner ───────────────────────────────────────────────────────────────

    def _spin_tick(self):
        self._spin_idx = (self._spin_idx + 1) % len(_SPINNER)
        try:
            self._spin_lbl.config(text=_SPINNER[self._spin_idx])
        except tk.TclError:
            return
        if not self._scan_done:
            self._spin_job = self.after(80, self._spin_tick)
        else:
            try:
                self._spin_lbl.config(text="✓")
            except tk.TclError:
                pass

    # ── drain event queue (called repeatedly via after()) ─────────────────────

    def _drain_queue(self):
        try:
            while True:
                event = self._event_q.get_nowait()
                self._handle_event(event)
        except queue.Empty:
            pass
        if not self._scan_done:
            self.after(50, self._drain_queue)

    def _handle_event(self, event: dict):
        kind = event["kind"]

        if kind == "progress":
            phase = event["phase"]
            done  = event["done"]
            total = event["total"]
            pct   = done / total if total else 0
            try:
                self._phase_lbl.config(text=phase)
                self._pct_lbl.config(text=f"{done:,} / {total:,}")
                fill_w = max(0, int(self._prog_bar_width * pct))
                self._prog_fill.place(x=0, y=0, width=fill_w, height=6)
            except tk.TclError:
                pass

        elif kind == "exact":
            group = {
                "label": f"Exact duplicate — MD5 {event['hash'][:8]}…",
                "kind":  "exact",
                "paths": event["files"],
                "scores": None,
            }
            self._groups.append(group)
            self._add_inbox_row(group)

        elif kind == "near":
            scores = {k: event[k] for k in
                      ("score_tfidf", "score_levenshtein", "score_sequence", "combined_score")}
            group = {
                "label":  f"Content similarity — {scores['combined_score']:.0%} combined",
                "kind":   "near",
                "paths":  [event["file_a"], event["file_b"]],
                "scores": scores,
            }
            self._groups.append(group)
            self._add_inbox_row(group)

        elif kind == "done":
            self._all_files = event["all_files"]
            self._scan_done = True
            self._on_scan_finished()

        elif kind == "error":
            messagebox.showerror("Scan error", event["msg"])
            self._show_landing()

    # ── add one row to the inbox ──────────────────────────────────────────────

    def _add_inbox_row(self, group: dict):
        # Remove "waiting" placeholder on first result
        if self._empty_lbl:
            self._empty_lbl.destroy()
            self._empty_lbl = None

        idx   = len(self._groups) - 1
        paths = group["paths"]
        kind  = group["kind"]

        row = tk.Frame(self._inbox_frame, bg=SURFACE,
                       highlightbackground=BORDER, highlightthickness=1)
        row.pack(fill="x", padx=16, pady=(8, 0))
        inner = tk.Frame(row, bg=SURFACE)
        inner.pack(fill="x", padx=12, pady=10)

        # badge
        accent    = DANGER if kind == "exact" else AMBER
        accent_lt = DANGER_LT if kind == "exact" else AMBER_LT
        _badge(inner, "Exact" if kind == "exact" else "Similar",
               accent_lt, accent).pack(side="left", padx=(0, 12))

        # label + file names
        info = tk.Frame(inner, bg=SURFACE)
        info.pack(side="left", fill="x", expand=True)
        _label(info, group["label"], font=FONT_BOLD).pack(anchor="w")
        names = "  ·  ".join(os.path.basename(p) for p in paths)
        _label(info, names, font=FONT_SMALL, color=TEXT_MUTED,
               wraplength=600).pack(anchor="w")

        # decision badge — updated when user reviews
        group["_decision_lbl"] = _label(inner, "Pending",
                                        font=FONT_SMALL, color=TEXT_MUTED)
        group["_decision_lbl"].pack(side="right", padx=(12, 0))

        # review button
        _btn(inner, "Review →",
             lambda i=idx: self._open_review(i),
             bg=ACCENT_LT, fg=ACCENT).pack(side="right")

        # update finish-button state
        self._refresh_finish_btn()

    # ── open review screen for one group ─────────────────────────────────────

    def _open_review(self, idx: int):
        group = self._groups[idx]
        paths = group["paths"]
        kind  = group["kind"]
        scores = group["scores"]

        # Save current inbox view, build review overlay
        self._review_window = tk.Toplevel(self)
        win = self._review_window
        win.title(f"Review — {group['label']}")
        win.geometry("1060x680")
        win.configure(bg=BG)
        win.grab_set()   # modal
        photos = []      # keep refs

        # ── top bar ───────────────────────────────────────────────────────────
        bar = tk.Frame(win, bg=SURFACE, highlightbackground=BORDER, highlightthickness=1)
        bar.pack(fill="x")
        bar_inner = tk.Frame(bar, bg=SURFACE)
        bar_inner.pack(fill="x", padx=16, pady=10)
        accent    = DANGER if kind == "exact" else AMBER
        accent_lt = DANGER_LT if kind == "exact" else AMBER_LT
        _badge(bar_inner, "Exact duplicate" if kind == "exact" else "Near-duplicate",
               accent_lt, accent).pack(side="left", padx=(0, 12))
        _label(bar_inner, group["label"], font=FONT_BOLD).pack(side="left")

        # ── scores strip ──────────────────────────────────────────────────────
        if kind == "near" and scores:
            sb = tk.Frame(win, bg=SURFACE, highlightbackground=BORDER, highlightthickness=1)
            sb.pack(fill="x")
            sb_inner = tk.Frame(sb, bg=SURFACE)
            sb_inner.pack(fill="x", padx=16, pady=8)
            _label(sb_inner, "Similarity:", font=FONT_SMALL, color=TEXT_MUTED
                   ).pack(side="left", padx=(0, 14))
            for method, key in (("TF-IDF","score_tfidf"),
                                 ("Levenshtein","score_levenshtein"),
                                 ("SequenceMatcher","score_sequence"),
                                 ("Combined","combined_score")):
                val   = scores[key]
                col   = SUCCESS if val >= 0.75 else (AMBER if val >= 0.45 else DANGER)
                chip  = tk.Frame(sb_inner, bg=SURFACE)
                chip.pack(side="left", padx=6)
                _label(chip, method, font=FONT_SMALL, color=TEXT_MUTED).pack(anchor="w")
                _label(chip, f"{val:.0%}", font=FONT_BOLD, color=col).pack(anchor="w")

        # hint
        hint = tk.Frame(win, bg=AMBER_LT, highlightbackground=BORDER, highlightthickness=1)
        hint.pack(fill="x")
        _label(hint, "Toggle which files to KEEP, then click Save decision.",
               font=FONT_SMALL, color=AMBER, bg=AMBER_LT).pack(side="left", padx=14, pady=7)

        # ── file tiles ────────────────────────────────────────────────────────
        body = tk.Frame(win, bg=BG)
        body.pack(fill="both", expand=True, padx=24, pady=20)

        step_vars: dict[str, tk.BooleanVar] = {}
        for path in paths:
            prev = self._decisions.get(path, False)
            step_vars[path] = tk.BooleanVar(value=prev)

        tiles = tk.Frame(body, bg=BG)
        tiles.pack(fill="both", expand=True)
        for col, path in enumerate(paths):
            tiles.columnconfigure(col, weight=1, uniform="tile")
            self._file_tile(tiles, path, col, step_vars[path], photos)

        # ── bottom actions ────────────────────────────────────────────────────
        act = tk.Frame(win, bg=SURFACE, highlightbackground=BORDER, highlightthickness=1)
        act.pack(fill="x", side="bottom")
        act_inner = tk.Frame(act, bg=SURFACE)
        act_inner.pack(fill="x", padx=20, pady=12)

        def _save(keep_none=False, keep_all=False):
            if keep_none:
                for v in step_vars.values(): v.set(False)
            elif keep_all:
                for v in step_vars.values(): v.set(True)
            for path, var in step_vars.items():
                self._decisions[path] = var.get()
            # Update inbox row label
            kept_names = [os.path.basename(p) for p, v in step_vars.items() if v.get()]
            if not kept_names:
                dec_text = "Skip all"
                dec_col  = DANGER
            elif len(kept_names) == len(paths):
                dec_text = "Keep all"
                dec_col  = SUCCESS
            else:
                dec_text = f"Keep: {', '.join(kept_names)}"
                dec_col  = ACCENT
            try:
                group["_decision_lbl"].config(text=dec_text, fg=dec_col)
            except tk.TclError:
                pass
            self._refresh_finish_btn()
            win.destroy()

        _btn(act_inner, "Skip all",
             lambda: _save(keep_none=True), bg=SURFACE, fg=DANGER).pack(side="left", padx=(0, 8))
        _btn(act_inner, "Keep all",
             lambda: _save(keep_all=True), bg=SURFACE, fg=SUCCESS).pack(side="left")
        _btn(act_inner, "Save decision ✓",
             lambda: _save(), bg=ACCENT, fg=SURFACE, bold=True).pack(side="right")
        _btn(act_inner, "Cancel",
             win.destroy, bg=SURFACE).pack(side="right", padx=(0, 8))

        # keep photo refs alive for this window
        win._photos = photos

    # ── file tile (used in review window) ────────────────────────────────────

    def _file_tile(self, grid, path: str, col: int,
                   var: tk.BooleanVar, photos: list):
        tile = tk.Frame(grid, bg=SURFACE, highlightbackground=BORDER, highlightthickness=1)
        tile.grid(row=0, column=col, padx=8, pady=4, sticky="nsew")

        photo = _render_pdf(path)
        if photo:
            photos.append(photo)
            img = tk.Label(tile, image=photo, bg=SURFACE, cursor="hand2")
            img.pack(padx=10, pady=(10, 4))
            img.bind("<Button-1>", lambda e, p=path: _open_file(p))
        else:
            ph = tk.Frame(tile, bg=GRAY_TILE, width=320, height=200)
            ph.pack(padx=10, pady=(10, 4))
            ph.pack_propagate(False)
            ext = Path(path).suffix.upper() or "FILE"
            tk.Label(ph, text=ext, font=("Georgia", 30, "bold"),
                     fg=GRAY_ICON, bg=GRAY_TILE, cursor="hand2"
                     ).place(relx=0.5, rely=0.5, anchor="center")
            ph.bind("<Button-1>", lambda e, p=path: _open_file(p))

        _label(tile, os.path.basename(path), font=FONT_BODY,
               wraplength=300).pack(padx=10, anchor="w", pady=(2, 0))
        try:
            sz = os.path.getsize(path)
            size_str = f"{sz/1024:.1f} KB" if sz < 1_048_576 else f"{sz/1_048_576:.1f} MB"
        except OSError:
            size_str = "—"
        _label(tile, size_str, font=FONT_SMALL, color=TEXT_MUTED
               ).pack(padx=10, anchor="w", pady=(0, 4))
        tk.Frame(tile, bg=BORDER, height=1).pack(fill="x", padx=10)

        bot = tk.Frame(tile, bg=SURFACE)
        bot.pack(fill="x", padx=10, pady=8)

        def _refresh(v=var, b=None):
            kept = v.get()
            b.config(
                text="✓  Keep this file" if kept else "  Keep this file",
                bg=SUCCESS_LT if kept else SURFACE,
                fg=SUCCESS if kept else TEXT_MUTED,
                highlightbackground=SUCCESS if kept else BORDER,
            )

        toggle = tk.Button(bot, text="  Keep this file", font=FONT_SMALL,
                           fg=TEXT_MUTED, bg=SURFACE, activebackground=SURFACE,
                           relief="flat", bd=0, padx=10, pady=5, cursor="hand2",
                           highlightbackground=BORDER, highlightthickness=1)
        toggle.config(command=lambda v=var, b=toggle: [v.set(not v.get()), _refresh(v, b)])
        toggle.pack(side="left", fill="x", expand=True, padx=(0, 6))
        _refresh(var, toggle)
        var.trace_add("write", lambda *_, v=var, b=toggle: _refresh(v, b))
        _btn(bot, "Open ↗", lambda p=path: _open_file(p),
             bg=SURFACE, fg=ACCENT).pack(side="right")

    # ── scan finished ─────────────────────────────────────────────────────────

    def _on_scan_finished(self):
        try:
            self._phase_lbl.config(text="Scan complete")
            self._pct_lbl.config(text=f"{len(self._groups)} group(s) found")
            self._prog_fill.place(x=0, y=0, width=self._prog_bar_width, height=6)
            if not self._groups:
                if self._empty_lbl:
                    self._empty_lbl.config(
                        text="✓  No duplicates found — every file is unique.",
                        font=FONT_BODY, fg=SUCCESS)
            self._refresh_finish_btn()
            self._status_lbl.config(
                text=f"Scan done — {len(self._groups)} group(s) found.")
        except tk.TclError:
            pass

    # ── finish button state ───────────────────────────────────────────────────

    def _refresh_finish_btn(self):
        """Enable Finish once scan is done AND every group has been reviewed."""
        try:
            if not self._scan_done:
                self._finish_btn.config(state="disabled")
                return
            all_decided = all(
                any(p in self._decisions for p in g["paths"])
                for g in self._groups
            )
            self._finish_btn.config(
                state="normal" if (not self._groups or all_decided) else "disabled"
            )
        except (tk.TclError, AttributeError):
            pass

    # ── copy & finish ─────────────────────────────────────────────────────────

    def _copy_output(self):
        out = os.path.join(self._folder, "work")
        os.makedirs(out, exist_ok=True)

        dup_files = set()
        for g in self._groups:
            for p in g["paths"]:
                dup_files.add(p)

        copied = 0
        for f in self._all_files:
            if f not in dup_files:
                shutil.copy2(f, os.path.join(out, os.path.basename(f)))
                copied += 1

        kept    = [p for p, keep in self._decisions.items() if keep]
        skipped = len(dup_files) - len(kept)
        for f in kept:
            shutil.copy2(f, os.path.join(out, os.path.basename(f)))
            copied += 1

        messagebox.showinfo(
            "Done",
            f"Output folder:\n{out}\n\n"
            f"{copied} file(s) copied\n"
            f"{len(kept)} duplicate(s) kept, {skipped} discarded.",
        )
        self._show_landing()

    # ── utility ───────────────────────────────────────────────────────────────

    def _clear(self):
        if self._spin_job:
            self.after_cancel(self._spin_job)
            self._spin_job = None
        for w in self.winfo_children():
            w.destroy()
        self._empty_lbl = None


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not PREVIEW_OK:
        print("Note: PDF previews disabled.  pip install PyMuPDF Pillow\n")

    app = FileUI()

    if len(sys.argv) >= 2:
        folder_arg = sys.argv[1]
        thresh_arg = float(sys.argv[2]) if len(sys.argv) > 2 else 0.85
        if os.path.isdir(folder_arg):
            app.after(100, lambda: app._start_scan(folder_arg, thresh_arg))
        else:
            print(f"Error: '{folder_arg}' is not a valid folder.")

    app.mainloop()
