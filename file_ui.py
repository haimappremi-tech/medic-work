#!/usr/bin/env python3
"""
file_ui.py
----------
Tkinter GUI for reviewing and resolving duplicate files one group at a time.
All detection logic lives in check_similar_files.py.

Flow
----
  Landing  →  [scan]  →  Step 1-of-N (decide)  →  Step 2-of-N  →  …  →  Summary & copy

Usage
-----
    python3 file_ui.py                    # opens folder-picker dialog
    python3 file_ui.py <folder>           # scan immediately
    python3 file_ui.py <folder> 0.90      # custom threshold

Requirements
------------
    pip install PyMuPDF Pillow            # for PDF previews (optional)
"""

import os
import sys
import shutil
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from pathlib import Path

from check_similar_files import find_similar_files

try:
    import fitz
    from PIL import Image, ImageTk
    PREVIEW_OK = True
except ImportError:
    PREVIEW_OK = False


# ── palette ──────────────────────────────────────────────────────────────────
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
FONT_STEP  = ("Georgia", 22, "bold")


# ── helpers ───────────────────────────────────────────────────────────────────

def _card(parent, bg=SURFACE, **kw):
    return tk.Frame(parent, bg=bg,
                    highlightbackground=BORDER, highlightthickness=1, **kw)


def _label(parent, text, font=FONT_BODY, color=TEXT, bg=None, **kw):
    bg = bg if bg is not None else parent["bg"]
    return tk.Label(parent, text=text, font=font, fg=color, bg=bg,
                    anchor="w", **kw)


def _badge(parent, text, bg, fg):
    return tk.Label(parent, text=text, font=FONT_SMALL,
                    fg=fg, bg=bg, padx=8, pady=3)


def _btn(parent, text, command, bg=SURFACE, fg=TEXT, bold=False, width=None):
    f = ("Georgia", 10, "bold") if bold else ("Georgia", 10)
    kw = {}
    if width:
        kw["width"] = width
    return tk.Button(
        parent, text=text, command=command,
        font=f, fg=fg, bg=bg,
        activebackground=bg, activeforeground=fg,
        relief="flat", bd=0, padx=14, pady=7,
        cursor="hand2", highlightbackground=BORDER, highlightthickness=1,
        **kw,
    )


def _render_pdf(path: str, width: int = 320):
    if not PREVIEW_OK or not path.lower().endswith(".pdf"):
        return None
    try:
        doc = fitz.open(path)
        page = doc[0]
        mat = fitz.Matrix(width / page.rect.width, width / page.rect.width)
        pix = page.get_pixmap(matrix=mat, alpha=False)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
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


# ── app ───────────────────────────────────────────────────────────────────────

class FileUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Duplicate File Finder")
        self.geometry("1100x760")
        self.minsize(860, 580)
        self.configure(bg=BG)
        self._photos = []
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

        _label(wrap, "Similarity threshold (for near-duplicates)",
               font=FONT_SMALL, color=TEXT_MUTED).pack(anchor="w")
        self._thresh = tk.DoubleVar(value=0.85)
        row = tk.Frame(wrap, bg=BG)
        row.pack(fill="x", pady=(4, 22))
        ttk.Scale(row, from_=0.5, to=1.0, variable=self._thresh,
                  orient="horizontal", length=280).pack(side="left")
        pct = _label(row, "85%", font=FONT_SMALL, color=TEXT_MUTED)
        pct.pack(side="left", padx=10)
        self._thresh.trace_add("write",
            lambda *_: pct.config(text=f"{self._thresh.get():.0%}"))

        _btn(wrap, "  Choose folder & scan  →",
             self._pick_and_scan, bg=ACCENT, fg=SURFACE, bold=True
             ).pack(pady=4, ipadx=6)

    # ── scan ──────────────────────────────────────────────────────────────────

    def _pick_and_scan(self):
        folder = filedialog.askdirectory(title="Select folder to scan")
        if folder:
            self._run_scan(folder, self._thresh.get())

    def _run_scan(self, folder: str, threshold: float):
        self._clear()
        wait = _label(self, "Scanning for duplicates…", font=FONT_TITLE, color=TEXT_MUTED)
        wait.place(relx=0.5, rely=0.5, anchor="center")
        self.update_idletasks()

        exact, near, all_files = find_similar_files(folder, threshold)
        wait.destroy()

        self._folder    = folder
        self._all_files = all_files

        # Build a flat list of groups: (label, kind, [paths], scores_dict|None)
        groups = []
        for h, paths in exact.items():
            groups.append((f"Exact duplicate  —  MD5 {h[:8]}…", "exact", paths, None))
        for nd in near:
            scores = {
                "score_tfidf":       nd["score_tfidf"],
                "score_levenshtein": nd["score_levenshtein"],
                "score_sequence":    nd["score_sequence"],
                "combined_score":    nd["combined_score"],
            }
            label = f"Content similarity  —  {nd['combined_score']:.0%} combined"
            groups.append((label, "near", [nd["file_a"], nd["file_b"]], scores))

        self._groups    = groups
        self._decisions = {}   # path -> bool (keep?)

        # All files that appear in ANY group
        self._dup_files: set = set()
        for _, _, paths, _ in groups:
            for p in paths:
                self._dup_files.add(p)

        if not groups:
            self._show_all_unique(folder)
        else:
            self._show_step(0)

    # ── no duplicates ─────────────────────────────────────────────────────────

    def _show_all_unique(self, folder: str):
        self._clear()
        wrap = tk.Frame(self, bg=BG)
        wrap.place(relx=0.5, rely=0.45, anchor="center")
        _label(wrap, "All clear — no duplicates found",
               font=FONT_HEAD, color=SUCCESS).pack()
        _label(wrap, f"Every file in  {folder}  is unique.",
               color=TEXT_MUTED).pack(pady=6)
        _btn(wrap, "← Back", self._show_landing).pack(pady=16)

    # ── step screen (one group) ───────────────────────────────────────────────

    def _show_step(self, idx: int):
        self._clear()
        self._photos.clear()

        total  = len(self._groups)
        label, kind, paths, scores = self._groups[idx]

        # ── top bar ──
        bar = tk.Frame(self, bg=SURFACE,
                       highlightbackground=BORDER, highlightthickness=1)
        bar.pack(fill="x", side="top")
        bar_inner = tk.Frame(bar, bg=SURFACE)
        bar_inner.pack(fill="x", padx=16, pady=10)

        _label(bar_inner, "Duplicate File Finder", font=FONT_TITLE).pack(side="left")

        right = tk.Frame(bar_inner, bg=SURFACE)
        right.pack(side="right")
        accent = DANGER if kind == "exact" else AMBER
        accent_lt = DANGER_LT if kind == "exact" else AMBER_LT
        _badge(right,
               "Exact duplicate" if kind == "exact" else "Near-duplicate",
               accent_lt, accent).pack(side="left", padx=(0, 10))
        _btn(right, "← Restart", self._show_landing).pack(side="left", padx=4)

        # ── progress strip ──
        prog = tk.Frame(self, bg=ACCENT_LT,
                        highlightbackground=BORDER, highlightthickness=1)
        prog.pack(fill="x", side="top")
        prog_inner = tk.Frame(prog, bg=ACCENT_LT)
        prog_inner.pack(fill="x", padx=14, pady=8)

        _label(prog_inner,
               f"Group {idx + 1} of {total}",
               font=FONT_BOLD, color=ACCENT, bg=ACCENT_LT).pack(side="left")
        _label(prog_inner,
               f"  —  {label}",
               font=FONT_SMALL, color=ACCENT, bg=ACCENT_LT).pack(side="left")

        # progress bar
        bar_bg = tk.Frame(prog_inner, bg=BORDER, height=6, width=200)
        bar_bg.pack(side="right", padx=(0, 4))
        bar_bg.pack_propagate(False)
        fill_w = max(4, int(200 * (idx + 1) / total))
        tk.Frame(bar_bg, bg=ACCENT, width=fill_w, height=6).place(x=0, y=0)

        # ── instruction strip ──
        hint = tk.Frame(self, bg=AMBER_LT,
                        highlightbackground=BORDER, highlightthickness=1)
        hint.pack(fill="x", side="top")
        _label(hint,
               "Check which files you want to KEEP, then click Next to continue.",
               font=FONT_SMALL, color=AMBER, bg=AMBER_LT,
               ).pack(side="left", padx=14, pady=7)

        # ── score breakdown (near-duplicates only) ───────────────────────────
        if kind == "near" and scores:
            score_bar = tk.Frame(self, bg=SURFACE,
                                 highlightbackground=BORDER, highlightthickness=1)
            score_bar.pack(fill="x", side="top")
            sb_inner = tk.Frame(score_bar, bg=SURFACE)
            sb_inner.pack(fill="x", padx=16, pady=8)
            _label(sb_inner, "Similarity scores:", font=FONT_SMALL,
                   color=TEXT_MUTED).pack(side="left", padx=(0, 14))
            for method, key, tip in (
                ("TF-IDF",        "score_tfidf",       "Semantic / topic overlap"),
                ("Levenshtein",   "score_levenshtein",  "Edit-distance on raw text"),
                ("SequenceMatcher","score_sequence",    "Longest common subsequences"),
            ):
                val = scores[key]
                col_fg = SUCCESS if val >= 0.75 else (AMBER if val >= 0.45 else DANGER)
                chip = tk.Frame(sb_inner, bg=SURFACE)
                chip.pack(side="left", padx=6)
                _label(chip, method, font=FONT_SMALL, color=TEXT_MUTED).pack(anchor="w")
                _label(chip, f"{val:.0%}", font=FONT_BOLD, color=col_fg).pack(anchor="w")
            # combined
            sep = tk.Frame(sb_inner, bg=BORDER, width=1, height=32)
            sep.pack(side="left", padx=10, fill="y")
            comb_chip = tk.Frame(sb_inner, bg=SURFACE)
            comb_chip.pack(side="left", padx=6)
            _label(comb_chip, "Combined", font=FONT_SMALL, color=TEXT_MUTED).pack(anchor="w")
            cv = scores["combined_score"]
            cv_fg = SUCCESS if cv >= 0.75 else (AMBER if cv >= 0.45 else DANGER)
            _label(comb_chip, f"{cv:.0%}", font=FONT_BOLD, color=cv_fg).pack(anchor="w")

        # ── file tiles ────────────────────────────────────────────────────────
        body = tk.Frame(self, bg=BG)
        body.pack(fill="both", expand=True, padx=24, pady=20)

        # Restore any previous decisions for this group
        step_vars: dict[str, tk.BooleanVar] = {}
        for path in paths:
            prev = self._decisions.get(path, False)
            step_vars[path] = tk.BooleanVar(value=prev)

        tiles_frame = tk.Frame(body, bg=BG)
        tiles_frame.pack(fill="both", expand=True)

        for col, path in enumerate(paths):
            tiles_frame.columnconfigure(col, weight=1, uniform="tile")
            self._file_tile(tiles_frame, path, col, step_vars[path])

        # ── bottom action bar ─────────────────────────────────────────────────
        act = tk.Frame(self, bg=SURFACE,
                       highlightbackground=BORDER, highlightthickness=1)
        act.pack(fill="x", side="bottom")
        act_inner = tk.Frame(act, bg=SURFACE)
        act_inner.pack(fill="x", padx=20, pady=12)

        # Skip — keep none from this group
        _btn(act_inner, "Skip (keep none)",
             lambda: self._advance(idx, paths, step_vars, keep_none=True),
             bg=SURFACE, fg=DANGER).pack(side="left", padx=(0, 8))

        # Keep all
        _btn(act_inner, "Keep all",
             lambda: self._advance(idx, paths, step_vars, keep_all=True),
             bg=SURFACE, fg=SUCCESS).pack(side="left")

        # Back (if not first)
        if idx > 0:
            _btn(act_inner, "← Back",
                 lambda: self._show_step(idx - 1),
                 bg=SURFACE).pack(side="right", padx=(8, 0))

        # Next / Finish
        is_last = (idx == total - 1)
        next_label = "Finish & copy →" if is_last else f"Next  ({idx + 2}/{total})  →"
        next_bg = ACCENT
        _btn(act_inner, next_label,
             lambda: self._advance(idx, paths, step_vars),
             bg=next_bg, fg=SURFACE, bold=True).pack(side="right")

    # ── file tile ─────────────────────────────────────────────────────────────

    def _file_tile(self, grid, path: str, col: int, var: tk.BooleanVar):
        tile = tk.Frame(grid, bg=SURFACE,
                        highlightbackground=BORDER, highlightthickness=1)
        tile.grid(row=0, column=col, padx=8, pady=4, sticky="nsew")

        # PDF preview or placeholder
        photo = _render_pdf(path)
        if photo:
            self._photos.append(photo)
            img = tk.Label(tile, image=photo, bg=SURFACE, cursor="hand2")
            img.pack(padx=10, pady=(10, 4))
            img.bind("<Button-1>", lambda e, p=path: _open_file(p))
        else:
            ph = tk.Frame(tile, bg=GRAY_TILE, width=320, height=200)
            ph.pack(padx=10, pady=(10, 4))
            ph.pack_propagate(False)
            ext = Path(path).suffix.upper() or "FILE"
            tk.Label(ph, text=ext, font=("Georgia", 30, "bold"),
                     fg=GRAY_ICON, bg=GRAY_TILE,
                     cursor="hand2").place(relx=0.5, rely=0.5, anchor="center")
            ph.bind("<Button-1>", lambda e, p=path: _open_file(p))

        # filename + size
        name = os.path.basename(path)
        _label(tile, name, font=FONT_BODY, color=TEXT,
               wraplength=300).pack(padx=10, anchor="w", pady=(2, 0))

        try:
            sz = os.path.getsize(path)
            size_str = (f"{sz/1024:.1f} KB" if sz < 1_048_576
                        else f"{sz/1_048_576:.1f} MB")
        except OSError:
            size_str = "—"
        _label(tile, size_str, font=FONT_SMALL, color=TEXT_MUTED
               ).pack(padx=10, anchor="w", pady=(0, 4))

        tk.Frame(tile, bg=BORDER, height=1).pack(fill="x", padx=10)

        # keep toggle + open
        bot = tk.Frame(tile, bg=SURFACE)
        bot.pack(fill="x", padx=10, pady=8)

        # Styled checkbox-like toggle button
        def _refresh_toggle(v=var, b=None):
            kept = v.get()
            b.config(
                text="✓  Keep this file" if kept else "  Keep this file",
                bg=SUCCESS_LT if kept else SURFACE,
                fg=SUCCESS if kept else TEXT_MUTED,
                highlightbackground=SUCCESS if kept else BORDER,
            )

        toggle = tk.Button(
            bot,
            text="  Keep this file",
            font=FONT_SMALL,
            fg=TEXT_MUTED, bg=SURFACE,
            activebackground=SURFACE,
            relief="flat", bd=0, padx=10, pady=5,
            cursor="hand2",
            highlightbackground=BORDER, highlightthickness=1,
        )
        toggle.config(command=lambda v=var, b=toggle: [v.set(not v.get()), _refresh_toggle(v, b)])
        toggle.pack(side="left", fill="x", expand=True, padx=(0, 6))
        _refresh_toggle(var, toggle)

        # Trace future changes (e.g. "Keep all" / "Skip")
        var.trace_add("write", lambda *_, v=var, b=toggle: _refresh_toggle(v, b))

        _btn(bot, "Open ↗", lambda p=path: _open_file(p),
             bg=SURFACE, fg=ACCENT).pack(side="right")

    # ── advance to next step ──────────────────────────────────────────────────

    def _advance(self, idx: int, paths: list, step_vars: dict,
                 keep_none: bool = False, keep_all: bool = False):
        # Override checkboxes if shortcut buttons used
        if keep_none:
            for v in step_vars.values():
                v.set(False)
        elif keep_all:
            for v in step_vars.values():
                v.set(True)

        # Save decisions
        for path, var in step_vars.items():
            self._decisions[path] = var.get()

        next_idx = idx + 1
        if next_idx < len(self._groups):
            self._show_step(next_idx)
        else:
            self._copy_output()

    # ── copy & finish ─────────────────────────────────────────────────────────

    def _copy_output(self):
        out = os.path.join(self._folder, "work")
        os.makedirs(out, exist_ok=True)

        copied = 0

        # Always copy non-duplicate files
        for f in self._all_files:
            if f not in self._dup_files:
                shutil.copy2(f, os.path.join(out, os.path.basename(f)))
                copied += 1

        # Copy user-chosen files from duplicate groups
        kept = [p for p, keep in self._decisions.items() if keep]
        for f in kept:
            shutil.copy2(f, os.path.join(out, os.path.basename(f)))
            copied += 1

        skipped = len(self._dup_files) - len(kept)
        messagebox.showinfo(
            "Done",
            f"Output folder created:\n{out}\n\n"
            f"{copied} file(s) copied\n"
            f"{len(kept)} duplicate(s) kept,  {skipped} discarded.",
        )
        self._show_landing()

    # ── utility ───────────────────────────────────────────────────────────────

    def _clear(self):
        for w in self.winfo_children():
            w.destroy()


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not PREVIEW_OK:
        print("Note: PDF previews are disabled.")
        print("  pip install PyMuPDF Pillow\n")

    app = FileUI()

    if len(sys.argv) >= 2:
        folder_arg = sys.argv[1]
        thresh_arg = float(sys.argv[2]) if len(sys.argv) > 2 else 0.85
        if os.path.isdir(folder_arg):
            app.after(100, lambda: app._run_scan(folder_arg, thresh_arg))
        else:
            print(f"Error: '{folder_arg}' is not a valid folder.")

    app.mainloop()
