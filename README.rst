aiogearman (work in progress)
=============================
.. image:: https://travis-ci.org/jettify/aiogearman.svg?branch=master
    :target: https://travis-ci.org/jettify/aiogearman
.. image:: https://coveralls.io/repos/jettify/aiogearman/badge.svg
    :target: https://coveralls.io/r/jettify/aiogearman


*aiogearman* -- is a library for accessing a gearman_ job server from
the asyncio (PEP-3156/tulip) framework.

gearman_ provides a generic application framework to farm out work to
other machines or processes that are better suited to do the work. It
allows you to do work in parallel, to load balance processing, and to
call functions between languages.


Requirements
------------

* Python_ 3.3+
* asyncio_ or Python_ 3.4+


License
-------

The *aiogearman* is offered under MIT license.

.. _Python: https://www.python.org
.. _asyncio: http://docs.python.org/3.4/library/asyncio.html
.. _gearman: http://gearman.org/
