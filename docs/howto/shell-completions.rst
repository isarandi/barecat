How to Set Up Shell Completions
================================

Barecat provides tab completion for bash and zsh shells.

Bash
----

Add to your ``~/.bashrc``:

.. code-block:: bash

   source $(barecat completion-script bash)

Or install system-wide:

.. code-block:: bash

   sudo cp $(barecat completion-script bash) /etc/bash_completion.d/barecat

Reload your shell:

.. code-block:: bash

   source ~/.bashrc

Zsh
---

Add to your ``~/.zshrc``:

.. code-block:: zsh

   source $(barecat completion-script zsh)

Or install to your completions directory:

.. code-block:: bash

   cp $(barecat completion-script zsh) ~/.zsh/completions/_barecat

Make sure your ``.zshrc`` includes:

.. code-block:: zsh

   fpath=(~/.zsh/completions $fpath)
   autoload -Uz compinit && compinit

Reload your shell:

.. code-block:: bash

   source ~/.zshrc

What Gets Completed
-------------------

Commands and aliases:

.. code-block:: bash

   barecat <TAB>
   # create  add  extract  list  ls  cat  shell  browse  du  verify  ...

Options:

.. code-block:: bash

   barecat create -<TAB>
   # -C  --directory  -T  --files-from  -0  --null  ...

Arguments:

.. code-block:: bash

   barecat create myarchive.barecat <TAB>
   # [shows files and directories]

   barecat completion-script <TAB>
   # bash  zsh

Verifying Installation
----------------------

.. code-block:: bash

   # Should show completion options
   barecat <TAB><TAB>

   # Should complete to "create"
   barecat cre<TAB>

Troubleshooting
---------------

Completions not working
~~~~~~~~~~~~~~~~~~~~~~~

1. Check the script exists:

   .. code-block:: bash

      barecat completion-script bash
      # Should print a path

2. Verify it's being sourced:

   .. code-block:: bash

      type _barecat
      # Should show "_barecat is a function"

3. Make sure bash-completion is installed:

   .. code-block:: bash

      # Debian/Ubuntu
      sudo apt install bash-completion

      # macOS
      brew install bash-completion

See Also
--------

- :doc:`../reference/cli` - CLI reference
