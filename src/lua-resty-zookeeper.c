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

typedef struct {
    zhandle_t *zk;
} zk_wrapper;

static zhandle_t *get_zhandle(lua_State *L) {
    lua_pushliteral(L, "zk_wrapper");
    lua_gettable(L, LUA_REGISTRYINDEX);
    zk_wrapper *wrapper = lua_touserdata(L, -1);

    if (wrapper == NULL) {
        lua_pushliteral(L, "cannot get zookeeper wrapper, maybe having not init?");
        lua_error(L);
    } else if (wrapper->zk == NULL) {
        lua_pushliteral(L, "cannot get zhandle, maybe having not init?");
        lua_error(L);
    }
    return wrapper->zk;
}

typedef struct {
    int errcode;
    const char *errmsg;
} zk_error;

static zk_error zk_errtab[] = {
    {ZOK, "ok"},
    {ZNONODE, "the parent node does not exist"},
    {ZNOAUTH, "the client does not have permission"},
    {ZNOTEMPTY, "children are present; node cannot be deleted"},
    {ZNODEEXISTS, "the node already exists"},
    {ZSYSTEMERROR, "a system (OS) error occured; it's worth checking errno to get details"},
    {ZBADARGUMENTS, "invalid input parameters"},
    {ZINVALIDSTATE, "ZOO_SESSION_EXPIRED_STATE or ZOO_AUTH_FAILED_STATE"},
    {ZCONNECTIONLOSS, "a network error occured while attempting to send request to server"},
    {ZMARSHALLINGERROR, "failed to marshall a request; possibly, out of memory"},
    {ZOPERATIONTIMEOUT, "failed to flush the buffers within the specified timeout"},
    {ZNOCHILDRENFOREPHEMERALS, "cannot create children of ephemeral nodes"},
    {0, NULL},
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

#define ZK_RETURN(L, errcode) do { \
    if (errcode == ZOK) { \
        lua_pushboolean(L, 1); \
    } else { \
        lua_pushboolean(L, 0); \
    } \
    lua_pushstring(L, zk_get_error(errcode)); \
    return 2; \
} while (0)

static void zk_watcher(zhandle_t *h, int type, int state,
        const char *path, void *ctx)
{
    lua_State *L = (lua_State *)ctx;

    // push watcher func
    lua_pushliteral(L, "zk_watcher");
    lua_gettable(L, LUA_REGISTRYINDEX);

    lua_pushliteral(L, "zk_wrapper");
    lua_gettable(L, LUA_REGISTRYINDEX);

    lua_pushinteger(L, type);
    lua_pushinteger(L, state);
    lua_pushstring(L, path);

    lua_call(L, 4, 0);
}

/* lua code:
local ok, errno = zk.init("127.0.0.1:2181")
if not ok then
    print("errno: ", errno)
end
*/
static int zk_init(lua_State *L)
{
    void *cb = NULL;
    size_t len = 0;
    const char *hosts = luaL_checklstring(L, 1, &len);

    int top = lua_gettop(L);

    if (top > 1) {
        // save watcher
        luaL_checktype(L, 2, LUA_TFUNCTION);
        lua_pushliteral(L, "zk_watcher");
        lua_pushvalue(L, 2);
        lua_settable(L, LUA_REGISTRYINDEX);
        cb = zk_watcher;
    }

    // save zookeeper handler
    zk_wrapper *wrapper = (zk_wrapper *)lua_newuserdata(L,
            sizeof(zk_wrapper));
    lua_pushliteral(L, "zk_wrapper");
    lua_pushlightuserdata(L, (void *)wrapper);
    lua_settable(L, LUA_REGISTRYINDEX);

    wrapper->zk = zookeeper_init(hosts, cb, 1000, NULL, (void *)L, 0);

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
    lua_pushliteral(L, "zk_wrapper");
    lua_gettable(L, LUA_REGISTRYINDEX);

    zk_wrapper *wrapper = lua_touserdata(L, -1);

    int rc = 0;
    if (wrapper != NULL) {
        rc = zookeeper_close(wrapper->zk);
    }

    lua_pushliteral(L, "zk_wrapper");
    lua_pushnil(L);
    lua_settable(L, LUA_REGISTRYINDEX);

    ZK_RETURN(L, rc);
}

static void empty_str_cb(int rc, const char *name, const void *data) {}

/*
lua code:
local ok, err = zk_acreate("/test", "some value")
 */
static int zk_acreate(lua_State *L)
{
    // get zhandle_t
    lua_pushliteral(L, "zk_wrapper");
    lua_gettable(L, LUA_REGISTRYINDEX);
    zk_wrapper *wrapper = lua_touserdata(L, -1);

    // get path
    const char *path = luaL_checkstring(L, 1);

    // get value
    size_t len;
    const char *val = luaL_checklstring(L, 2, &len);
    
    int rc = zoo_acreate(wrapper->zk, path, val, (int)len,
            &ZOO_OPEN_ACL_UNSAFE, 0, empty_str_cb, NULL);

    ZK_RETURN(L, rc);
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
    // get zhandle_t
    lua_pushliteral(L, "zk_wrapper");
    lua_gettable(L, LUA_REGISTRYINDEX);
    zk_wrapper *wrapper = lua_touserdata(L, -1);

    // get path
    const char *path = luaL_checkstring(L, 1);

    // get value
    size_t len;
    const char *val = luaL_checklstring(L, 2, &len);
    
    int rc = zoo_create(wrapper->zk, path, val, (int)len,
            &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);

    ZK_RETURN(L, rc);
}

/* lua code:
local ok, err = zk.delete("some_path")
if not ok then
    print("zk.delete error: ", err)
end
*/
static int zk_delete(lua_State *L)
{
    zhandle_t *zk = get_zhandle(L);
    const char *path = luaL_checkstring(L, 1);
    int rc = zoo_delete(zk, path, -1);
    ZK_RETURN(L, rc);
}

static const luaL_Reg zk[] = {
    {"init", zk_init},
    {"close", zk_close},
    {"acreate", zk_acreate},
    {"create", zk_create},
    {"delete", zk_delete},
    {NULL, NULL},
};

int luaopen_zk(lua_State *L)
{
    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);
    luaL_register(L, "zk", zk);

    return 1;
}
