#include <lua.h>
#include <lauxlib.h>
#include <luajit.h>
#include <lualib.h>
#include <luaconf.h>

#include <zookeeper/zookeeper.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <errno.h>
#include <pthread.h>

#define ZK_WRAPPER "zk_wrapper"
#define ZK_WATCHER "zk_watcher"
#define ZK_WTABLE "zk_wtable"

typedef struct {
    zhandle_t *zk;
    pthread_mutex_t mut;
} zk_wrapper;

static zk_wrapper *get_zk_wrapper(lua_State *L) {
    lua_pushliteral(L, ZK_WRAPPER);
    lua_gettable(L, LUA_REGISTRYINDEX);
    zk_wrapper *wrapper = lua_touserdata(L, -1);

    if (wrapper == NULL) {
        lua_pushliteral(L, "cannot get zookeeper wrapper, maybe having not init?");
        lua_error(L);
    } else if (wrapper->zk == NULL) {
        lua_pushliteral(L, "zhandle(NULL) maybe having not init?");
        lua_error(L);
    }
    return wrapper;
}

typedef struct {
    int errcode;
    const char *errmsg;
} zk_error;

static zk_error zk_errtab[] = {
    {ZOK, "ok"},
    {ZNOAUTH, "the client does not have permission"},
    {ZNONODE, "the parent node does not exist"},
    {ZCLOSING, "zookeeper is closing"},
    {ZNOTHING, "(not error) no server responses to process"},
    {ZAPIERROR, "api error"},
    {ZNOTEMPTY, "children are present; node cannot be deleted"},
    {ZAUTHFAILED, "client authentication specified"},
    {ZBADVERSION, "version conflict"},
    {ZINVALIDACL, "invalid ACL specified"},
    {ZNODEEXISTS, "the node already exists"},
    {ZSYSTEMERROR, "a system (OS) error occured; it's worth checking errno to get details"},
    {ZBADARGUMENTS, "invalid input parameters"},
    {ZINVALIDSTATE, "ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE"},
    {ZSESSIONMOVED, "session moved to another server, so operation is ignored"},
    {ZUNIMPLEMENTED, "operation is unimplemented"},
    {ZCONNECTIONLOSS, "connection to the server has been lost"},
    {ZSESSIONEXPIRED, "the session has been expired by the server"},
    {ZINVALIDCALLBACK, "invalid callback specified"},
    {ZMARSHALLINGERROR, "failed to marshall a request; possibly, out of memory"},
    {ZOPERATIONTIMEOUT, "operation timeout"},
    {ZDATAINCONSISTENCY, "a data inconsistency was found"},
    {ZRUNTIMEINCONSISTENCY, "a runtime inconsistency was found"},
    {ZNOCHILDRENFOREPHEMERALS, "cannot create children of ephemeral nodes"},
    {-32768, NULL},
};

static const char *zk_get_error(int errcode)
{
    const char *unknown = "unknown error";

    for (int i = 0; ; ++i) {
        zk_error *e = &zk_errtab[i];
        if (e->errmsg == NULL) {
            return unknown;
        }
        if (e->errcode == errcode) {
            return e->errmsg;
        }
    }
    return unknown;
}

#define ZK_RETURN_BOOL(L, errcode) do { \
    if (errcode == ZOK) { \
        lua_pushboolean(L, 1); \
    } else { \
        lua_pushboolean(L, 0); \
    } \
    lua_pushstring(L, zk_get_error(errcode)); \
    return 2; \
} while (0)

#define ZK_RETURN_LSTR(L, str, len, errcode) do { \
    if (errcode == ZOK) { \
        lua_pushlstring(L, str, (size_t)len); \
    } else { \
        lua_pushnil(L); \
    } \
    lua_pushstring(L, zk_get_error(errcode)); \
    return 2; \
} while (0)

static void zk_empty_watcher(zhandle_t *h, int type, int state,
        const char *path, void *ctx)
{
    lua_State *L = (lua_State *)ctx;
    zk_wrapper *wrapper = get_zk_wrapper(L);
    pthread_mutex_unlock(&(wrapper->mut));
}

static void zk_watcher(zhandle_t *h, int type, int state,
        const char *path, void *ctx)
{
    lua_State *L = (lua_State *)ctx;
    zk_wrapper *wrapper = get_zk_wrapper(L);

    lua_pushliteral(L, ZK_WATCHER);
    lua_gettable(L, LUA_REGISTRYINDEX);

    lua_pushinteger(L, type);
    lua_pushinteger(L, state);
    lua_pushstring(L, path);

    int args = 3;

    lua_pushliteral(L, ZK_WTABLE);
    lua_gettable(L, LUA_REGISTRYINDEX);

    int wtab_index = lua_gettop(L);

    // loop through the param table
    lua_pushnil(L);
    while (lua_next(L, wtab_index) != 0) {
        lua_pushvalue(L, -2); // push key into top of stack for next loop
        lua_remove(L, -3); // remove key, remain value
        ++args;

        lua_checkstack(L, 4); // to protect overflow
    }

    lua_remove(L, wtab_index);
    lua_call(L, args, 0);

    pthread_mutex_unlock(&(wrapper->mut));
}

/* lua code:
local ok, errno = zk.init("127.0.0.1:2181")

* or

local ok, errno = zk.init("127.0.0.1:2181", 
    function(type, state, path, ...) end, ...)
if not ok then
    print("errno: ", errno)
end
*/
static int zk_init(lua_State *L)
{
    void *cb = zk_empty_watcher;
    size_t len = 0;
    const char *hosts = luaL_checklstring(L, 1, &len);

    int top = lua_gettop(L);

    if (top >= 2) {
        // save watcher
        luaL_checktype(L, 2, LUA_TFUNCTION);
        lua_pushliteral(L, ZK_WATCHER);
        lua_pushvalue(L, 2);
        lua_settable(L, LUA_REGISTRYINDEX);
        cb = zk_watcher;

        if (top >= 3) {
            lua_pushliteral(L, ZK_WTABLE);
            lua_newtable(L);
            for (int t = 3; t <= top; ++t) {
                lua_pushinteger(L, t - 2);
                lua_pushvalue(L, t);
                lua_settable(L, -3);
            }
            lua_settable(L, LUA_REGISTRYINDEX);
        }
    }

    // save zookeeper handler
    lua_pushliteral(L, ZK_WRAPPER);
    zk_wrapper *wrapper = (zk_wrapper *)lua_newuserdata(L,
            sizeof(zk_wrapper));
    lua_settable(L, LUA_REGISTRYINDEX);

    wrapper->zk = zookeeper_init(hosts, cb, 1000, NULL, (void *)L, 0);
    if (pthread_mutex_init(&(wrapper->mut), NULL)) {
        lua_pushstring(L, strerror(errno));
        lua_error(L);
    }

    if (wrapper->zk == NULL) {
        lua_pushboolean(L, 0);
        lua_pushinteger(L, errno);
    } else {
        lua_pushboolean(L, 1);
        lua_pushinteger(L, 0);
    }

    return 2;
}

static int zk_close(lua_State *L)
{
    lua_pushliteral(L, ZK_WRAPPER);
    lua_gettable(L, LUA_REGISTRYINDEX);
    zk_wrapper *wrapper = lua_touserdata(L, -1);

    int rc = 0;
    if (wrapper != NULL) {
        rc = zookeeper_close(wrapper->zk);
        pthread_mutex_destroy(&(wrapper->mut));
    }

    lua_pushliteral(L, ZK_WRAPPER);
    lua_pushnil(L);
    lua_settable(L, LUA_REGISTRYINDEX);

    ZK_RETURN_BOOL(L, rc);
}

static void empty_str_cb(int rc, const char *name, const void *data) {}

/*

WARNING: no stable

lua code:
local ok, err = zk_acreate("/test", "some value")
 */
static int zk_acreate(lua_State *L)
{
    zk_wrapper *wrapper = get_zk_wrapper(L);

    // get path
    const char *path = luaL_checkstring(L, 1);

    // get value
    size_t len;
    const char *val = luaL_checklstring(L, 2, &len);
    
    int rc = zoo_acreate(wrapper->zk, path, val, (int)len,
            &ZOO_OPEN_ACL_UNSAFE, 0, empty_str_cb, NULL);

    ZK_RETURN_BOOL(L, rc);
}

/* lua code:
local zk = require "zk"
zk.init("127.0.0.1:2181,127.0.0.1:2182,...")
local ok, err = zk.create("/test", "some value")
if not ok then
   print("zk.create node err: ", err)
end
 */
static int zk_create(lua_State *L)
{
    zk_wrapper *wrapper = get_zk_wrapper(L);

    // get path
    const char *path = luaL_checkstring(L, 1);

    // get value
    size_t len;
    const char *val = luaL_checklstring(L, 2, &len);
    
    int rc = zoo_create(wrapper->zk, path, val, (int)len,
            &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);

    pthread_mutex_lock(&(wrapper->mut));

    ZK_RETURN_BOOL(L, rc);
}

/* lua code:
local ok, err = zk.delete("some_path")
if not ok then
    print("zk.delete error: ", err)
end
*/
static int zk_delete(lua_State *L)
{
    zk_wrapper *wrapper = get_zk_wrapper(L);

    const char *path = luaL_checkstring(L, 1);
    int rc = zoo_delete(wrapper->zk, path, -1);

    // wait for call back
    pthread_mutex_lock(&(wrapper->mut));

    ZK_RETURN_BOOL(L, rc);
}

/*
local ok, err = zk.set("path", "data")
if not ok then
    print("zk.set error: ", err)
end
*/
static int zk_set(lua_State *L)
{
    zk_wrapper *wrapper = get_zk_wrapper(L);

    const char *path = luaL_checkstring(L, 1);
    size_t data_len = 0;
    const char *data = luaL_checklstring(L, 2, &data_len);
    int rc = zoo_set(wrapper->zk, path, data, data_len, -1);

    pthread_mutex_lock(&(wrapper->mut));

    ZK_RETURN_BOOL(L, rc);
}

/*
local data, err = zk.get("path")
if not data then
    print("zk.get error: ", err)
end
*/
static int zk_get(lua_State *L)
{
    zk_wrapper *wrapper = get_zk_wrapper(L);

    const char *path = luaL_checkstring(L, 1);
    char buf[1024];
    int size = sizeof(buf);

    int rc = zoo_get(wrapper->zk, path, 1, buf, &size, NULL);

    if (rc == ZOK) {
        ZK_RETURN_LSTR(L, buf, size, rc);
    } else {
        ZK_RETURN_BOOL(L, rc);
    }
}

/*
local ok, err = zk.set_log_level(zk.ZOO_LOG_LEVEL_DEBUG)
if not ok then
    print("set log level error: ", err)
end
*/
static int zk_set_log_level(lua_State *L)
{
    int level = luaL_checkinteger(L, 1);
    switch (level) {
        case ZOO_LOG_LEVEL_DEBUG:
        case ZOO_LOG_LEVEL_INFO:
        case ZOO_LOG_LEVEL_WARN:
        case ZOO_LOG_LEVEL_ERROR:
            break;
        default:
            lua_pushboolean(L, 0);
            lua_pushliteral(L, "illegal log level");
            return 2;
    }
    zoo_set_debug_level((ZooLogLevel)level);
    lua_pushboolean(L, 1);
    lua_pushnil(L);
    return 2;
}

#define ZK_VAL_IF(name, s) \
    if (name == s) { \
        lua_pushliteral(L, #s); \
        break; \
    }

#define FUNC_ZK_STR_BEGIN(name) \
static int zk_##name##_str(lua_State *L) \
{ \
    int name = luaL_checkinteger(L, 1); \
    do {

#define FUNC_ZK_STR_END(name) \
        break; \
    } while (1); \
    return 1; \
}

// function: static int zk_state_str(lua_State *L)
FUNC_ZK_STR_BEGIN(state)
    ZK_VAL_IF(state, ZOO_EXPIRED_SESSION_STATE)
    ZK_VAL_IF(state, ZOO_AUTH_FAILED_STATE)
    ZK_VAL_IF(state, ZOO_CONNECTING_STATE)
    ZK_VAL_IF(state, ZOO_ASSOCIATING_STATE)
    ZK_VAL_IF(state, ZOO_CONNECTED_STATE)
FUNC_ZK_STR_END(state)

// function: static int zk_event_str(lua_State *L)
FUNC_ZK_STR_BEGIN(event)
    ZK_VAL_IF(event, ZOO_CREATED_EVENT)
    ZK_VAL_IF(event, ZOO_DELETED_EVENT)
    ZK_VAL_IF(event, ZOO_CHANGED_EVENT)
    ZK_VAL_IF(event, ZOO_CHILD_EVENT)
    ZK_VAL_IF(event, ZOO_SESSION_EVENT)
    ZK_VAL_IF(event, ZOO_NOTWATCHING_EVENT)
FUNC_ZK_STR_END(event)

#undef ZK_VAL_IF
#undef FUNC_ZK_STR_BEGIN
#undef FUNC_ZK_STR_END

static const luaL_Reg zk[] = {
    {"init", zk_init},
    {"close", zk_close},
    {"acreate", zk_acreate},
    {"create", zk_create},
    {"delete", zk_delete},
    {"set", zk_set},
    {"get", zk_get},
    {"set_log_level", zk_set_log_level},
    {"state_str", zk_state_str},
    {"event_str", zk_event_str},
    {NULL, NULL},
};

int luaopen_zk(lua_State *L)
{
    // stack: ["zk"]

    // default setting
    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);

    luaL_register(L, "zk", zk);
    // stack: ["zk", table(zk)]

    lua_newtable(L);
    // stack: ["zk", table(zk), {}]

    lua_pushliteral(L, "__index");
    // stack: ["zk", table(zk), {}, "__index"]

    lua_newtable(L);
    // stack: ["zk", table(zk), {}, "__index", {}]

#define SET_T(l, k) do { \
    lua_pushliteral(l, #k); \
    lua_pushinteger(l, k); \
    lua_settable(l, -3); \
} while (0)

    SET_T(L, ZOO_LOG_LEVEL_DEBUG);
    SET_T(L, ZOO_LOG_LEVEL_INFO);
    SET_T(L, ZOO_LOG_LEVEL_WARN);
    SET_T(L, ZOO_LOG_LEVEL_ERROR);

    SET_T(L, ZOO_CREATED_EVENT);
    SET_T(L, ZOO_DELETED_EVENT);
    SET_T(L, ZOO_CHANGED_EVENT);
    SET_T(L, ZOO_CHILD_EVENT);
    SET_T(L, ZOO_SESSION_EVENT);
    SET_T(L, ZOO_NOTWATCHING_EVENT);

    SET_T(L, ZOO_EXPIRED_SESSION_STATE);
    SET_T(L, ZOO_AUTH_FAILED_STATE);
    SET_T(L, ZOO_CONNECTING_STATE);
    SET_T(L, ZOO_ASSOCIATING_STATE);
    SET_T(L, ZOO_CONNECTED_STATE);

#undef SET_T
    /* 
    stack: ["zk", table(zk), {}, "__index", {
        "ZOO_LOG_LEVEL_DEBUG", ZOO_LOG_LEVEL_DEBUG,
        ...
    }]
    */

    lua_settable(L, -3);
    /* 
    stack: ["zk", table(zk), {"__index" = {
        "ZOO_LOG_LEVEL_DEBUG" = ZOO_LOG_LEVEL_DEBUG,
        ...
    }}]
    */

    lua_setmetatable(L, -2);
    // stack: ["zk", table(zk)]

    return 1;
}
