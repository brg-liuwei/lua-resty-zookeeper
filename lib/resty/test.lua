local zk = require "zk"

local ok, err = zk.init("127.0.0.1:2181")
if not ok then
    print("init err: ", err)
    return
end
-- zk.init("127.0.0.1:2181", function() print("callback") end)

local ok, err = zk.create("/test5", "welcome to use zookeeper to manage your meta data")

if not ok then
    print("create error: ", err)
end

-- local ok, err = zk.set("/test4", "this is my data")
-- if not ok then
--     print("set error: ", err)
-- end

local data, err = zk.get("/test5")
if not data then
    print("get error: ", err)
else
    print("get data: ", data)
end

local ok, err = zk.close()
if not ok then
    print("close err: ", err)
    return
end
