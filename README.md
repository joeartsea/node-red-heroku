node-red-heroku
================

A wrapper for deploying [Node-RED](http://nodered.org) into the [Heroku](https://www.heroku.com).

### Deploying Node-RED into Heroku

[![Deploy](https://www.herokucdn.com/deploy/button.png)](https://heroku.com/deploy?template=https://github.com/joeartsea/node-red-heroku)

### Password protect the flow editor

By default, the editor is open for anyone to access and modify flows. To password-protect the editor:

Add the following user-defined variables.

* NODE_RED_USERNAME - the username to secure the editor with
* NODE_RED_PASSWORD - the password to secure the editor with
