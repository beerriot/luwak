{application, luwak,
 [
  {description, "luwak"},
  {vsn, "1.1.0"},
  {modules, [
             luwak_app,
             luwak_io,
             luwak_block,
             luwak_file,
             luwak_tree,
             luwak_tree_utils,
             luwak_put_stream,
             luwak_get_stream,
             luwak_checksum
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib,
                  skerl
                 ]},
  {mod, { luwak_app, []}},
  {env, []}
 ]}.
