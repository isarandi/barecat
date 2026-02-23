"""Ranger-like terminal file browser for barecat archives."""

import curses
import os.path as osp
from typing import Optional

import barecat


class BarecatBrowser:
    """Curses-based file browser for barecat archives."""

    def __init__(self, archive_path: str):
        self.archive_path = archive_path
        self.bc = barecat.Barecat(archive_path, readonly=True, readonly_is_immutable=False)
        self.cwd = ''  # Current directory in archive
        self.cursor = 0  # Selected item index
        self.scroll_offset = 0  # Scroll position in file list
        self.preview_offset = 0  # Scroll position in preview
        self.preview_lines: list[str] = []  # Cached preview lines
        self.preview_path: Optional[str] = None  # Currently previewed path
        self.preview_chunk_size = 4096  # Bytes to read at a time
        self.preview_loaded_bytes = 0  # How much of file we've loaded
        self.entries: list[tuple[str, bool]] = []  # (name, is_dir) pairs
        self.focus = 'list'  # 'list' or 'preview'
        self._load_entries()

    def _load_entries(self):
        """Load directory entries for current directory."""
        try:
            names = list(self.bc.listdir(self.cwd))
        except KeyError:
            names = []

        # Separate dirs and files, sort each
        dirs = []
        files = []
        for name in names:
            full_path = osp.join(self.cwd, name) if self.cwd else name
            if self.bc.index.isdir(full_path):
                dirs.append((name, True))
            else:
                files.append((name, False))

        self.entries = sorted(dirs) + sorted(files)
        self.cursor = min(self.cursor, max(0, len(self.entries) - 1))
        self.scroll_offset = 0
        self._reset_preview()

    def _reset_preview(self):
        """Reset preview state."""
        self.preview_lines = []
        self.preview_path = None
        self.preview_loaded_bytes = 0
        self.preview_offset = 0

    def _get_selected_path(self) -> Optional[str]:
        """Get full path of selected item."""
        if not self.entries:
            return None
        name, _ = self.entries[self.cursor]
        return osp.join(self.cwd, name) if self.cwd else name

    def _get_parent_entries(self) -> list[tuple[str, bool]]:
        """Get entries of parent directory."""
        if not self.cwd:
            return []
        parent = osp.dirname(self.cwd)
        try:
            names = list(self.bc.listdir(parent))
        except KeyError:
            return []

        entries = []
        for name in names:
            full_path = osp.join(parent, name) if parent else name
            is_dir = self.bc.index.isdir(full_path)
            entries.append((name, is_dir))
        return sorted([e for e in entries if e[1]]) + sorted([e for e in entries if not e[1]])

    def _load_preview_chunk(self, max_lines: int, max_width: int = 40):
        """Load more preview content if needed."""
        selected = self._get_selected_path()
        if not selected:
            return

        # Check if we need to reset preview
        if selected != self.preview_path:
            self._reset_preview()
            self.preview_path = selected

        # If it's a directory, show its contents (like ranger)
        if self.entries and self.entries[self.cursor][1]:
            if not self.preview_lines:
                try:
                    names = list(self.bc.listdir(selected))
                    # Separate and sort dirs and files
                    dirs = []
                    files = []
                    for name in names:
                        full_path = osp.join(selected, name) if selected else name
                        if self.bc.index.isdir(full_path):
                            dirs.append(name + '/')
                        else:
                            files.append(name)
                    self.preview_lines = sorted(dirs) + sorted(files)
                    if not self.preview_lines:
                        self.preview_lines = ['[Empty directory]']
                except KeyError:
                    self.preview_lines = ['[Cannot read directory]']
            return

        # For text/binary files, load incrementally
        try:
            info = self.bc.index.lookup_file(selected)
            file_size = info.size

            # Check if we need more lines
            needed_lines = self.preview_offset + max_lines + 10
            while len(self.preview_lines) < needed_lines and self.preview_loaded_bytes < file_size:
                # Read next chunk
                chunk_start = self.preview_loaded_bytes
                chunk_size = min(self.preview_chunk_size, file_size - chunk_start)
                data = self.bc.sharder.read_from_address(
                    info.shard, info.offset + chunk_start, chunk_size
                )
                self.preview_loaded_bytes += len(data)

                # Try to decode as text
                try:
                    text = data.decode('utf-8', errors='replace')
                    # Split into lines, handling partial lines
                    new_lines = text.split('\n')
                    if self.preview_lines and not self.preview_lines[-1].endswith('\n'):
                        # Append to last incomplete line
                        self.preview_lines[-1] += new_lines[0]
                        new_lines = new_lines[1:]
                    self.preview_lines.extend(new_lines)
                except Exception:
                    # Binary file
                    self.preview_lines = [f'[Binary file: {self._format_size(file_size)}]']
                    break

            # Add EOF marker if fully loaded
            if self.preview_loaded_bytes >= file_size and self.preview_lines:
                if not any('[EOF]' in line for line in self.preview_lines[-3:]):
                    self.preview_lines.append('')
                    self.preview_lines.append(f'[EOF - {self._format_size(file_size)}]')

        except KeyError:
            self.preview_lines = ['[Cannot read file]']

    def _format_size(self, size: int) -> str:
        """Format size in human-readable form."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f'{size:.1f} {unit}' if unit != 'B' else f'{size} {unit}'
            size /= 1024
        return f'{size:.1f} TB'

    def _draw(self, stdscr):
        """Draw the interface."""
        stdscr.erase()  # erase() is less flickery than clear()
        height, width = stdscr.getmaxyx()

        # Calculate column widths (roughly 20% / 35% / 45%)
        col1_width = max(15, width // 5)
        col2_width = max(20, width * 35 // 100)
        col3_width = width - col1_width - col2_width - 2  # -2 for borders
        # Store for mouse click detection
        self._col1_width = col1_width
        self._col2_width = col2_width
        self._height = height

        # Draw column borders
        for y in range(height - 1):
            try:
                stdscr.addch(y, col1_width, curses.ACS_VLINE)
                stdscr.addch(y, col1_width + col2_width + 1, curses.ACS_VLINE)
            except curses.error:
                pass

        # Draw parent directory (left column)
        parent_entries = self._get_parent_entries()
        current_dir_name = osp.basename(self.cwd) if self.cwd else ''
        for i, (name, is_dir) in enumerate(parent_entries[: height - 1]):
            if i >= height - 1:
                break
            display = name[: col1_width - 1]
            attr = curses.A_BOLD if is_dir else 0
            if name == current_dir_name:
                attr |= curses.A_REVERSE
            try:
                stdscr.addnstr(i, 0, display + ('/' if is_dir else ''), col1_width - 1, attr)
            except curses.error:
                pass

        # Draw current directory (middle column)
        visible_height = height - 1
        # Adjust scroll to keep cursor visible
        if self.cursor < self.scroll_offset:
            self.scroll_offset = self.cursor
        elif self.cursor >= self.scroll_offset + visible_height:
            self.scroll_offset = self.cursor - visible_height + 1

        x_offset = col1_width + 1
        for i, (name, is_dir) in enumerate(
            self.entries[self.scroll_offset : self.scroll_offset + visible_height]
        ):
            y = i
            if y >= height - 1:
                break
            display = name[: col2_width - 1]
            attr = curses.A_BOLD if is_dir else 0
            if i + self.scroll_offset == self.cursor:
                attr |= curses.A_REVERSE
            suffix = '/' if is_dir else ''
            try:
                stdscr.addnstr(y, x_offset, display + suffix, col2_width - 1, attr)
            except curses.error:
                pass

        # Draw preview (right column)
        x_offset = col1_width + col2_width + 2
        self._load_preview_chunk(visible_height, col3_width)

        preview_attr = curses.A_DIM if self.focus == 'list' else 0
        for i, line in enumerate(
            self.preview_lines[self.preview_offset : self.preview_offset + visible_height]
        ):
            if i >= height - 1:
                break
            # Truncate and clean line for display
            display_line = line.replace('\t', '    ')[: col3_width - 1]
            display_line = ''.join(c if c.isprintable() or c == ' ' else '?' for c in display_line)
            try:
                stdscr.addnstr(i, x_offset, display_line, col3_width - 1, preview_attr)
            except curses.error:
                pass

        # Draw status bar
        status = f" {self.cwd or '/'} | {len(self.entries)} items"
        if self.entries:
            selected = self._get_selected_path()
            if selected and not self.entries[self.cursor][1]:
                try:
                    info = self.bc.index.lookup_file(selected)
                    status += f' | {self._format_size(info.size)}'
                except KeyError:
                    pass
        status += ' | q:quit h/l:nav j/k:move'
        try:
            stdscr.addnstr(height - 1, 0, status[: width - 1], width - 1, curses.A_REVERSE)
        except curses.error:
            pass

        stdscr.refresh()

    def _handle_input(self, stdscr, key: int) -> bool:
        """Handle keyboard input. Returns False to quit."""
        if key == ord('q'):
            return False

        elif key in (ord('j'), curses.KEY_DOWN):
            if self.focus == 'list':
                if self.cursor < len(self.entries) - 1:
                    self.cursor += 1
                    self._reset_preview()
            else:
                self.preview_offset += 1

        elif key in (ord('k'), curses.KEY_UP):
            if self.focus == 'list':
                if self.cursor > 0:
                    self.cursor -= 1
                    self._reset_preview()
            else:
                self.preview_offset = max(0, self.preview_offset - 1)

        elif key in (ord('l'), curses.KEY_RIGHT, ord('\n'), curses.KEY_ENTER):
            if self.focus == 'list' and self.entries:
                name, is_dir = self.entries[self.cursor]
                if is_dir:
                    # Enter directory
                    self.cwd = osp.join(self.cwd, name) if self.cwd else name
                    self.cursor = 0
                    self._load_entries()
                else:
                    # Switch focus to preview
                    self.focus = 'preview'

        elif key in (ord('h'), curses.KEY_LEFT):
            if self.focus == 'preview':
                self.focus = 'list'
            elif self.cwd:
                # Go to parent directory
                old_dir = osp.basename(self.cwd)
                self.cwd = osp.dirname(self.cwd)
                self._load_entries()
                # Try to select the directory we came from
                for i, (name, is_dir) in enumerate(self.entries):
                    if name == old_dir:
                        self.cursor = i
                        break

        elif key in (ord('g'), curses.KEY_HOME):
            if self.focus == 'list':
                self.cursor = 0
                self._reset_preview()
            else:
                self.preview_offset = 0

        elif key in (ord('G'), curses.KEY_END):
            if self.focus == 'list':
                self.cursor = max(0, len(self.entries) - 1)
                self._reset_preview()

        elif key == curses.KEY_PPAGE:  # Page Up
            if self.focus == 'list':
                self.cursor = max(0, self.cursor - 10)
                self._reset_preview()
            else:
                self.preview_offset = max(0, self.preview_offset - 10)

        elif key == curses.KEY_NPAGE:  # Page Down
            if self.focus == 'list':
                self.cursor = min(len(self.entries) - 1, self.cursor + 10)
                self._reset_preview()
            else:
                self.preview_offset += 10

        elif key == ord('\t'):
            # Toggle focus between list and preview
            self.focus = 'preview' if self.focus == 'list' else 'list'

        elif key == curses.KEY_MOUSE:
            try:
                _, mx, my, _, bstate = curses.getmouse()
                self._handle_mouse(mx, my, bstate)
            except curses.error:
                pass

        return True

    def _handle_mouse(self, mx: int, my: int, bstate: int):
        """Handle mouse click at position (mx, my)."""
        # Ignore clicks on status bar
        if my >= self._height - 1:
            return

        # Determine which column was clicked
        col1_end = self._col1_width
        col2_start = self._col1_width + 1
        col2_end = col2_start + self._col2_width
        col3_start = col2_end + 1

        if bstate & curses.BUTTON1_CLICKED or bstate & curses.BUTTON1_DOUBLE_CLICKED:
            if mx < col1_end:
                # Clicked in parent column - go up
                if self.cwd:
                    old_dir = osp.basename(self.cwd)
                    self.cwd = osp.dirname(self.cwd)
                    self._load_entries()
                    for i, (name, is_dir) in enumerate(self.entries):
                        if name == old_dir:
                            self.cursor = i
                            break

            elif col2_start <= mx < col2_end:
                # Clicked in main file list
                clicked_idx = my + self.scroll_offset
                if 0 <= clicked_idx < len(self.entries):
                    if bstate & curses.BUTTON1_DOUBLE_CLICKED:
                        # Double click - enter directory or view file
                        self.cursor = clicked_idx
                        self._reset_preview()
                        name, is_dir = self.entries[self.cursor]
                        if is_dir:
                            self.cwd = osp.join(self.cwd, name) if self.cwd else name
                            self.cursor = 0
                            self._load_entries()
                    else:
                        # Single click - select
                        self.cursor = clicked_idx
                        self._reset_preview()
                    self.focus = 'list'

            elif mx >= col3_start:
                # Clicked in preview column
                self.focus = 'preview'

        elif bstate & curses.BUTTON4_PRESSED:  # Scroll up
            if self.focus == 'list':
                if self.cursor > 0:
                    self.cursor -= 3
                    self.cursor = max(0, self.cursor)
                    self._reset_preview()
            else:
                self.preview_offset = max(0, self.preview_offset - 3)

        elif bstate & curses.BUTTON5_PRESSED:  # Scroll down
            if self.focus == 'list':
                self.cursor += 3
                self.cursor = min(len(self.entries) - 1, self.cursor)
                self._reset_preview()
            else:
                self.preview_offset += 3

    def run(self, stdscr):
        """Main loop."""
        curses.curs_set(0)  # Hide cursor
        curses.use_default_colors()
        # Enable mouse support
        curses.mousemask(curses.ALL_MOUSE_EVENTS | curses.REPORT_MOUSE_POSITION)
        stdscr.timeout(100)  # Non-blocking input for responsive UI
        # Store column widths for mouse click detection
        self._col1_width = 0
        self._col2_width = 0

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
    """Entry point for barecat-browse command."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Browse barecat archive with ranger-like interface'
    )
    parser.add_argument('archive', help='Path to the barecat archive')
    args = parser.parse_args()

    browser = BarecatBrowser(args.archive)
    curses.wrapper(browser.run)


if __name__ == '__main__':
    main()
