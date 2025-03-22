API Reference
=============

Start at :class:`barecat.Barecat` to explore the API.

.. toctree::
   :titlesonly:

   {% for page in pages|selectattr("is_top_level_object") %}
   {{ page.include_path }}
   {% endfor %}
