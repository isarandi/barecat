"""Shell completion script utilities."""

import importlib.resources


def get_completion_script(shell='bash'):
    """Return the path to the shell completion script.

    Args:
        shell: 'bash' or 'zsh'

    Returns:
        Path to the completion script file.

    Example:
        # For bash, add to ~/.bashrc:
        source $(python -c "import barecat; print(barecat.get_completion_script('bash'))")

        # For zsh, add to ~/.zshrc:
        source $(python -c "import barecat; print(barecat.get_completion_script('zsh'))")
    """
    if shell == 'bash':
        return str(importlib.resources.files('barecat.completions').joinpath('barecat.bash'))
    elif shell == 'zsh':
        return str(importlib.resources.files('barecat.completions').joinpath('barecat.zsh'))
    else:
        raise ValueError(f"Unknown shell: {shell}. Use 'bash' or 'zsh'.")
