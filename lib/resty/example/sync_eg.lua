-- this is an example of using synchronize api of zookeeper

local zk = require "zk"

local wtab = {
    msg = "hello",
    usage = "example",
}

--[[
    by default, log level is set ZOO_LOG_LEVEL_ERROR, you can set log level as follows:

    zk.set_log_level(zk.ZOO_LOG_LEVEL_DEBUG)
    zk.set_log_level(zk.ZOO_LOG_LEVEL_INFO)
    zk.set_log_level(zk.ZOO_LOG_LEVEL_WARN)
    zk.set_log_level(zk.ZOO_LOG_LEVEL_ERROR)
]]

-- local ok, err = zk.init("127.0.0.1:2181")

-- local ok, err = zk.init("127.0.0.1:2181", function(type_, state, path, ...)
--     print(">>> watcher")
--     print("    callback type_: ", zk.event_str(type_))
--     print("    callback state: ", zk.state_str(state))
--     print("    callback path: ", path)
--     if type(...) == "table" then
--         for k, v in pairs(wtab) do
--             print("   ", k, " ==> ", v)
--         end
--     end
--     print()
-- end, wtab)

local a, b = "liu", "wei"
local tab = {"lua", "clang", browser = "chrome", "golang", os = "OSX", "erlang", }
local ok, err = zk.init("127.0.0.1:2181", function(type_, state, path, fir_name, sec_name, t)
     print(">>> watcher\n")
     print("    callback type_: ", zk.event_str(type_), "\n")
     -- print("    callback state: ", zk.state_str(state))
     -- print("    callback path: ", path)
     -- print("    first_name: ", fir_name)
     -- print("    second_name: ", sec_name)
     -- print("    table t:")
     -- for k, v in pairs(t) do
     --     print("        ", k, " ==> ", v)
     -- end
     print(" >>>>>>>> lua watcher end\n")
end, a, b, tab)
if not ok then
    print("init error: ", err)
end


print("create pre")
local ok, err = zk.create("/mytest", "welcome to use zookeeper to manage your meta data")
if not ok then
    print("create error: ", err)
end
print("create post")

wtab.phase = "after create"
wtab.usage = "to debug"

local data, err = zk.get("/mytest")
if not data then
    print("get error: ", err)
else
    print("get data: ", data)
end

local new_data = "new data"
local ok, err = zk.set("/mytest", new_data)
if not ok then
    print("set error: ", err)
else
    print("set new data ok: ", new_data)
end

local data, err = zk.get("/mytest")
if not data then
    print("get error: ", err)
else
    print("get data: ", data)
end

a, b = "new-first-name", "new-second-name"
tab[1] = "LUAJIT"
table.insert(tab, "new-elem1")
table.insert(tab, "new-elem2")

-- print("before delete, change a, b, tab, NOTICE: tab can be changed BUT string (see value of a, b) cannot be changed")

local ok, err = zk.delete("/mytest")
if not ok then
    print("delete /mytest error: ", err)
else
    print("delete /mytest ok")
end

local ok, err = zk.close()
if not ok then
    print("close err: ", err)
    return
end

print("close ok")

