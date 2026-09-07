# ag-crud-rethink
Realtime CRUD data management layer/plugin for SocketCluster using RethinkDB as the database.

See https://github.com/SocketCluster/ag-crud-sample for a full working sample.

## Setup

See https://github.com/SocketCluster/ag-crud-sample for sample app which demonstrates this component in action.

This module is a plugin for SocketCluster, so you need to have SocketCluster installed: https://socketcluster.io/
Once SocketCluster is installed and you have created a new project, you should navigate to your project's main directory and run:

```bash
npm install ag-crud-rethink --save
```

Now you will need to attach the plugin to your SocketCluster server - So open ```server.js``` and pass the `agServer` instance to the `attach` function exposed by this module.

As shown in the sample above, you will need to provide a schema for your data.
In the example above, the Category, Product, and User keys represent tables/models within RethinkDB - Inside each of these, you
need to declare what fields are allowed and optionally the **views** which are supported for each model type.

Simply put, a **view** is an ordered, filtered subset of all documents within a table. Views need to define a ```filter``` and/or ```order``` function
which will be used to construct the view for table's data.

## Options

- `schemaSettlementDelay` (Default: `5000`) - When multiple workers start up at the same time, RethinkDB may end up
creating more than one database or table with the same name. After creating a database or table, this module waits
this many milliseconds to allow concurrent workers to finish their own creations, then drops any duplicates - keeping
the copy with the smallest `id` so that all workers converge on the same one. The delay is only incurred when this
worker actually created a database or table.
