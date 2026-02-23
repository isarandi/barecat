"""ncdu-like disk usage viewer for barecat archives."""
import curses
import os.path as osp
from typing import Optional

import barecat


class BarecatDu:
    """Curses-based disk usage viewer for barecat archives."""

    def __init__(self, archive_path: str):
        self.archive_path = archive_path
        self.bc = barecat.Barecat(archive_path, readonly=True)
        self.cwd = ''
        self.cursor = 0
        self.scroll_offset = 0
        self.entries: list[tuple[str, int, int, bool]] = []  # (name, size, count, is_dir)
        self.path_stack: list[tuple[str, int]] = []  # For going back: (path, cursor_pos)
        self._load_entries()

    def _load_entries(self):
        """Load directory entries sorted by size (largest first)."""
        entries = []

        # Add .. entry if not at root
        if self.cwd:
            entries.append(('..', 0, 0, True))  # (name, size, count, is_dir)

        # Get subdirectories with their tree sizes
        subdirs = []
        for dinfo in self.bc.index.list_subdir_dirinfos(self.cwd):
            name = osp.basename(dinfo.path)
            subdirs.append((name, dinfo.size_tree, dinfo.num_files_tree, True))

        # Get files with their sizes
        files = []
        for finfo in self.bc.index.list_direct_fileinfos(self.cwd):
            name = osp.basename(finfo.path)
            files.append((name, finfo.size, 1, False))

        # Sort by size descending and append
        entries.extend(sorted(subdirs, key=lambda x: -x[1]))
        entries.extend(sorted(files, key=lambda x: -x[1]))

        self.entries = entries
        self.cursor = min(self.cursor, max(0, len(self.entries) - 1))
        self.scroll_offset = 0

    def _get_total_size(self) -> int:
        """Get total size of current directory."""
        try:
            dinfo = self.bc.index.lookup_dir(self.cwd)
            return dinfo.size_tree
        except KeyError:
            return sum(size for _, size, _, _ in self.entries)

    def _get_total_count(self) -> int:
        """Get total file count of current directory."""
        try:
            dinfo = self.bc.index.lookup_dir(self.cwd)
            return dinfo.num_files_tree
        except KeyError:
            return sum(count for _, _, count, _ in self.entries)

    def _format_size(self, size: int) -> str:
        """Format size in human-readable form (ncdu style)."""
        if size < 1024:
            return f"{size:5} B"
        for unit in ['K', 'M', 'G', 'T', 'P']:
            size /= 1024
            if size < 1000:
                return f"{size:5.1f} {unit}iB"
        return f"{size:5.1f} EiB"

    def _format_count(self, count: int) -> str:
        """Format file count (right-aligned, 6 chars)."""
        if count == 0:
            return "      "
        if count < 1000:
            return f"{count:>6}"
        elif count < 1000000:
            return f"{count/1000:>5.1f}K"
        else:
            return f"{count/1000000:>5.1f}M"

    def _draw(self, stdscr):
        """Draw the interface."""
        stdscr.erase()
        height, width = stdscr.getmaxyx()

        # Header
        header = f"--- {self.archive_path}: {self.cwd or '/'} "
        header += "-" * (width - len(header) - 1)
        try:
            stdscr.addnstr(0, 0, header[:width-1], width-1)
        except curses.error:
            pass

        # Calculate column widths
        total_size = self._get_total_size()
        size_col_width = 12  # "999.9 GiB "
        count_col_width = 7  # " 999.9K"
        bar_width = 20
        name_start = size_col_width + count_col_width + bar_width + 4

        # Column headers (aligned with data columns)
        try:
            stdscr.addnstr(1, 0, f"{'Size':>{size_col_width}}", size_col_width, curses.A_DIM)
            stdscr.addnstr(1, size_col_width, f"{'Files':>{count_col_width}}", count_col_width, curses.A_DIM)
            stdscr.addnstr(1, name_start, "Name", width - name_start - 1, curses.A_DIM)
        except curses.error:
            pass

        # Entries
        visible_height = height - 4  # Header + column header + footer
        if self.cursor < self.scroll_offset:
            self.scroll_offset = self.cursor
        elif self.cursor >= self.scroll_offset + visible_height:
            self.scroll_offset = self.cursor - visible_height + 1

        for i, (name, size, count, is_dir) in enumerate(
            self.entries[self.scroll_offset:self.scroll_offset + visible_height]
        ):
            y = i + 2  # +2 for header and column header
            if y >= height - 1:
                break

            idx = i + self.scroll_offset
            attr = curses.A_REVERSE if idx == self.cursor else 0

            # Special handling for ..
            if name == '..':
                try:
                    stdscr.addnstr(y, 0, " " * (size_col_width + count_col_width + bar_width + 2),
                                  size_col_width + count_col_width + bar_width + 2, attr)
                    stdscr.addnstr(y, name_start, "/..", width - name_start - 1, attr | curses.A_BOLD)
                except curses.error:
                    pass
                continue

            # Size
            size_str = self._format_size(size)
            try:
                stdscr.addnstr(y, 0, size_str, size_col_width, attr)
            except curses.error:
                pass

            # Count
            count_str = self._format_count(count)
            try:
                stdscr.addnstr(y, size_col_width, count_str, count_col_width, attr)
            except curses.error:
                pass

            # Bar
            if total_size > 0:
                bar_fill = int(bar_width * size / total_size)
            else:
                bar_fill = 0
            bar = "[" + "#" * bar_fill + " " * (bar_width - bar_fill) + "]"
            try:
                stdscr.addnstr(y, size_col_width + count_col_width, bar, bar_width + 2, attr)
            except curses.error:
                pass

            # Name
            display_name = ("/" + name) if is_dir else ("  " + name)
            try:
                stdscr.addnstr(y, name_start, display_name[:width - name_start - 1],
                              width - name_start - 1, attr | (curses.A_BOLD if is_dir else 0))
            except curses.error:
                pass

        # Footer with total
        total_count = self._get_total_count()
        total_str = f" Total: {self._format_size(total_size)} | {total_count} files"
        total_str += " | q:quit  h/<-:back  l/->:enter  j/k:move"
        try:
            stdscr.addnstr(height - 1, 0, total_str[:width-1], width-1, curses.A_REVERSE)
        except curses.error:
            pass

        stdscr.refresh()

    def _handle_input(self, stdscr, key: int) -> bool:
        """Handle keyboard input. Returns False to quit."""
        if key == ord('q'):
            return False

        elif key in (ord('j'), curses.KEY_DOWN):
            if self.cursor < len(self.entries) - 1:
                self.cursor += 1

        elif key in (ord('k'), curses.KEY_UP):
            if self.cursor > 0:
                self.cursor -= 1

        elif key in (ord('l'), curses.KEY_RIGHT, ord('\n'), curses.KEY_ENTER):
            if self.entries:
                name, size, count, is_dir = self.entries[self.cursor]
                if name == '..':
                    # Go up
                    self._go_up()
                elif is_dir:
                    self.path_stack.append((self.cwd, self.cursor))
                    self.cwd = osp.join(self.cwd, name) if self.cwd else name
                    self.cursor = 0
                    self._load_entries()

        elif key in (ord('h'), curses.KEY_LEFT):
            self._go_up()

        elif key in (ord('g'), curses.KEY_HOME):
            self.cursor = 0

        elif key in (ord('G'), curses.KEY_END):
            self.cursor = max(0, len(self.entries) - 1)

        elif key == curses.KEY_PPAGE:
            self.cursor = max(0, self.cursor - 10)

        elif key == curses.KEY_NPAGE:
            self.cursor = min(len(self.entries) - 1, self.cursor + 10)

        elif key == curses.KEY_MOUSE:
            try:
                _, mx, my, _, bstate = curses.getmouse()
                if bstate & curses.BUTTON1_CLICKED:
                    clicked_idx = my - 2 + self.scroll_offset  # -2 for headers
                    if 0 <= clicked_idx < len(self.entries):
                        self.cursor = clicked_idx
                elif bstate & curses.BUTTON1_DOUBLE_CLICKED:
                    clicked_idx = my - 2 + self.scroll_offset
                    if 0 <= clicked_idx < len(self.entries):
                        self.cursor = clicked_idx
                        name, size, count, is_dir = self.entries[self.cursor]
                        if name == '..':
                            self._go_up()
                        elif is_dir:
                            self.path_stack.append((self.cwd, self.cursor))
                            self.cwd = osp.join(self.cwd, name) if self.cwd else name
                            self.cursor = 0
                            self._load_entries()
                elif bstate & curses.BUTTON4_PRESSED:
                    self.cursor = max(0, self.cursor - 3)
                elif bstate & curses.BUTTON5_PRESSED:
                    self.cursor = min(len(self.entries) - 1, self.cursor + 3)
            except curses.error:
                pass

        return True

    def _go_up(self):
        """Navigate to parent directory."""
        if self.path_stack:
            self.cwd, self.cursor = self.path_stack.pop()
            self._load_entries()
        elif self.cwd:
            old_dir = osp.basename(self.cwd)
            self.cwd = osp.dirname(self.cwd)
            self._load_entries()
            # Try to select the directory we came from
            for i, (name, _, _, is_dir) in enumerate(self.entries):
                if name == old_dir and is_dir:
                    self.cursor = i
                    break

    def run(self, stdscr):
        """Main loop."""
        curses.curs_set(0)
        curses.use_default_colors()
        curses.mousemask(curses.ALL_MOUSE_EVENTS | curses.REPORT_MOUSE_POSITION)
        stdscr.timeout(100)

        running = True
        while running:
            self._draw(stdscr)
            try:
                key = stdscr.getch()
                if key != -1:
                    running = self._handle_input(stdscr, key)
            except KeyboardInterrupt:
                break

        self.bc.close()


def main():
    """Entry point for barecat-du command."""
    import argparse
    parser = argparse.ArgumentParser(
        description='ncdu-like disk usage viewer for barecat archives'
    )
    parser.add_argument('archive', help='Path to the barecat archive')
    args = parser.parse_args()

    du = BarecatDu(args.archive)
    curses.wrapper(du.run)


if __name__ == '__main__':
    main()
