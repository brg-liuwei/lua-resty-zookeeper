local zk = require "zk"

local ok, err = zk.init("127.0.0.1:2181", function(a, b, c, d) end, nil)
if not ok then
    print("init error: ", err)
    return
end

local ok, err = zk.create("/nginx", "yes")
if not ok then
    print("create /nginx err: ", err)
end

local ok, err = zk.delete("/nginx")
if not ok then
    print("delete /nginx err: ", err)
end

local ok, err = zk.close()
if not ok then
    print("close error: ", err)
end
