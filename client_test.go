package luckypool

import (
	"testing"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	srv1 = "localhost:11211"
	srv2 = "localhost:11212"
	srv3 = "localhost:11213"
)

func Test_PoolManager_AddPools(t *testing.T) {
	c, err := New(srv1)
	require.NoError(t, err)

	c.AddPools([]string{srv2, srv3})
	assert.Len(t, c.servers, 2)
}

func TestClient_Add(t *testing.T) {
	c, err := New(srv1)
	defer c.CloseAll()
	require.NoError(t, err)

	c.AddPools([]string{srv2, srv3})

	err = c.Set("key", []byte("hogehoge"))
	assert.NoError(t, err)

	mc1 := memcache.New(srv1)
	defer mc1.Close()
	item, err := mc1.Get("key")
	require.NoError(t, err)
	assert.Equal(t, item.Value, []byte("hogehoge"))

	mc2 := memcache.New(srv2)
	defer mc2.Close()
	item, err = mc2.Get("key")
	require.NoError(t, err)
	assert.Equal(t, item.Value, []byte("hogehoge"))
}

func TestClient_Get(t *testing.T) {
	c, err := New(srv1)
	defer c.CloseAll()
	require.NoError(t, err)

	err = c.Set("key", []byte("hogehoge"))
	assert.NoError(t, err)

	actual, err := c.Get("key")
	assert.NoError(t, err)

	mc := memcache.New(srv1)
	defer mc.Close()
	expect, err := mc.Get("key")
	require.NoError(t, err)
	assert.Equal(t, expect.Value, actual)
}

func TestClient_Delete(t *testing.T) {
	c, err := New(srv1)
	defer c.CloseAll()
	require.NoError(t, err)

	c.AddPools([]string{srv2, srv3})

	err = c.Set("key", []byte("hogehoge"))
	assert.NoError(t, err)

	mc := memcache.New(srv1)
	defer mc.Close()

	_, err = mc.Get("key")
	require.NoError(t, err)

	err = c.Delete("key")
	assert.NoError(t, err)

	_, err = mc.Get("key")
	require.Equal(t, memcache.ErrCacheMiss, err)
}
