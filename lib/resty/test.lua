local zk = require "zk"

local ok, err = zk.init("127.0.0.1:2181")
if not ok then
    print("init err: ", err)
    return
end
-- zk.init("127.0.0.1:2181", function() print("callback") end)

local ok, err = zk.create("/test4", "hello world")

if not ok then
    print("create error: ", err)
end

local ok, err = zk.create("/test4", "hello world")

if not ok then
    print("create error: ", err)
end

local ok, err = zk.delete("/test4")
if not ok then
    print("delete error: ", err)
end

local ok, err = zk.delete("/test4")
if not ok then
    print("delete error: ", err)
end

local ok, err = zk.close()
if not ok then
    print("close err: ", err)
    return
end
