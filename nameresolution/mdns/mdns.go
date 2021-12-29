// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package mdns

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/grandcat/zeroconf"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/dapr/kit/logger"
)

const (
	// firstOnlyTimeout 是浏览单个应用程序ID的第一个响应时使用的超时。
	firstOnlyTimeout = time.Second * 1
	// refreshTimeout is the timeout used when
	// browsing for any responses to a single app id.
	refreshTimeout = time.Second * 3
	// refreshInterval is the duration between
	// background address refreshes.
	refreshInterval = time.Second * 30
	// addressTTL 域名解析的有效时长
	addressTTL = time.Second * 60
	// 这个架构上支持的最大整数值。
	maxInt = int(^uint(0) >> 1)
)

// address 是用来存储一个IP地址和一个过期时间，在这个时间点上，该地址被认为是过期的，不能信任。
type address struct {
	ip        string
	expiresAt time.Time
}

// addressList 代表一组地址，以及用于控制和访问所述地址的数据。
type addressList struct {
	addresses []address // 已经按照日期从大到小排序好了            9点 8点 7点 6点 5点  4点
	counter   int
	mu        sync.RWMutex // 应用级别的锁
}

// expire 删除任何过期时间早于当前时间的地址。
func (a *addressList) expire() {
	a.mu.Lock()
	defer a.mu.Unlock()

	i := 0
	for _, addr := range a.addresses {
		if time.Now().Before(addr.expiresAt) {
			a.addresses[i] = addr
			i++
		}
	}
	a.addresses = a.addresses[:i]
}

// add 将一个新的地址添加到地址列表中，并规定了最长的有效期。对于现有的地址，过期时间被更新为最大值。
// TODO: 考虑 最大的地址列表长度。
func (a *addressList) add(ip string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	// 如果该地址已经存在，则刷新过期时间
	for i := range a.addresses {
		if a.addresses[i].ip == ip {
			a.addresses[i].expiresAt = time.Now().Add(addressTTL)
			return
		}
	}
	// 不存在，则添加
	a.addresses = append(a.addresses, address{
		ip:        ip,
		expiresAt: time.Now().Add(addressTTL),
	})
}

// next 从列表中获取下一个地址，考虑到当前的轮回实现。除了尽力而为的线性迭代，对选择没有任何保证。
func (a *addressList) next() *string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if len(a.addresses) == 0 {
		return nil
	}

	if a.counter == maxInt {
		// 防止溢出
		a.counter = 0
	}
	index := a.counter % len(a.addresses)
	addr := a.addresses[index]
	a.counter++

	return &addr.ip
}

// NewResolver 创建mdns解析的实例
func NewResolver(logger logger.Logger) nameresolution.Resolver {
	r := &resolver{
		appAddressesIPv4: make(map[string]*addressList),
		appAddressesIPv6: make(map[string]*addressList),
		refreshChan:      make(chan string),
		logger:           logger,
	}

	// 按需刷新所有的应用程序地址。
	go func() {
		for appID := range r.refreshChan {
			if err := r.refreshApp(context.Background(), appID); err != nil {
				r.logger.Warnf(err.Error())
			}
		}
	}()

	// 定期刷新所有的应用程序地址。
	go func() {
		for {
			time.Sleep(refreshInterval)

			if err := r.refreshAllApps(context.Background()); err != nil {
				r.logger.Warnf(err.Error())
			}
		}
	}()

	return r
}

type resolver struct {
	ipv4Mu           sync.RWMutex
	appAddressesIPv4 map[string]*addressList // 应用ID : &实例列表
	ipv6Mu           sync.RWMutex
	appAddressesIPv6 map[string]*addressList // 应用ID : &实例列表
	refreshChan      chan string
	logger           logger.Logger
}

var _ nameresolution.Resolver = &resolver{}

// Init 注册mdns服务
func (m *resolver) Init(metadata nameresolution.Metadata) error {
	var appID string
	var hostAddress string
	var ok bool
	var instanceID string

	props := metadata.Properties

	if appID, ok = props[nameresolution.MDNSInstanceName]; !ok {
		return errors.New("没有名称")
	}
	if hostAddress, ok = props[nameresolution.MDNSInstanceAddress]; !ok {
		return errors.New("没有地址")
	}

	p, ok := props[nameresolution.MDNSInstancePort]
	if !ok {
		return errors.New("没有端口")
	}

	port, err := strconv.ParseInt(p, 10, 32)
	if err != nil {
		return errors.New("端口无效")
	}

	if instanceID, ok = props[nameresolution.MDNSInstanceID]; !ok {
		instanceID = ""
	}

	err = m.registerMDNS(instanceID, appID, []string{hostAddress}, int(port))
	if err == nil {
		m.logger.Infof("公布本地服务入口: %s -> %s:%d", appID, hostAddress, port)
	}

	return err
}

// 相当于是启动了mdns的服务端
func (m *resolver) registerMDNS(instanceID string, appID string, ips []string, port int) error {
	started := make(chan bool, 1)
	var err error

	go func() {
		var server *zeroconf.Server

		host, _ := os.Hostname()
		info := []string{appID}

		// 默认的实例ID对于进程来说是唯一的。
		if instanceID == "" {
			instanceID = fmt.Sprintf("%s-%d", host, syscall.Getpid())
		}

		if len(ips) > 0 {
			// 注册一个服务代理。这个调用将跳过主机名/IP查找，并使用提供的值。
			server, err = zeroconf.RegisterProxy(instanceID, appID, "local.", port, host, ips, info, nil)
		} else {
			// 一个服务的给定参数。这个调用将获取系统的主机名并通过该主机名查找IP。
			server, err = zeroconf.Register(instanceID, appID, "local.", port, info, nil)
		}

		if err != nil {
			started <- false
			m.logger.Errorf("注册 zeroconf 错误: %s", err)

			return
		}
		started <- true

		// 等待,直到收到SIGTERM事件.
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig

		server.Shutdown()
	}()

	<-started

	return err
}

// ResolveID 通过mDNS将名称解析为地址。
func (m *resolver) ResolveID(req nameresolution.ResolveRequest) (string, error) {
	// 首先检查这个应用程序ID的缓存IPv4地址。
	if addr := m.nextIPv4Address(req.ID); addr != nil {
		return *addr, nil
	}

	// 首先检查这个应用程序ID的缓存IPv6地址
	if addr := m.nextIPv6Address(req.ID); addr != nil {
		return *addr, nil
	}

	// 缓存缺失，则退回到浏览网络寻找地址。
	m.logger.Debugf("缓存中没有找到mDNS地址，浏览网络的应用ID %s", req.ID)

	// 得到我们收到的第一个地址...
	addr, err := m.browseFirstOnly(context.Background(), req.ID)
	if err == nil {
		// 触发后台刷新
		m.refreshChan <- req.ID
	}

	return addr, err
}

// browseFirstOnly 将执行一次mDNS网络浏览，寻找与所提供的应用程序ID相匹配的地址。它将返回它收到的第一个地址，并停止浏览任何其他地址。
func (m *resolver) browseFirstOnly(ctx context.Context, appID string) (string, error) {
	var addr string

	ctx, cancel := context.WithTimeout(ctx, firstOnlyTimeout) // 1秒
	defer cancel()

	//onFirst将在收到的第一个地址上被调用。由于cancel()的异步性，不能保证它只在第一个地址上被调用。确保这个函数的多次调用是安全的。
	onFirst := func(ip string) {
		addr = ip
		cancel() // 停止browse
	}

	m.logger.Debugf("浏览应用程序ID的第一个mDNS地址 %s", appID)

	err := m.browse(ctx, appID, onFirst)
	if err != nil {
		return "", err
	}

	// 等到上下文被取消或超时。
	<-ctx.Done()

	if errors.Is(ctx.Err(), context.Canceled) {
		m.logger.Debugf("取消了对应用程序id %s的第一个mDNS地址的浏览。", appID)
	} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		m.logger.Debugf("对应用程序id %s的第一个mDNS地址的浏览 超时了。", appID)
	}

	if addr == "" {
		return "", fmt.Errorf("不能发现服务地址: %s", appID)
	}

	return addr, nil
}

// refreshApp 将对所提供的应用程序ID进行mDNS网络浏览。 这个函数是封锁的。
func (m *resolver) refreshApp(ctx context.Context, appID string) error {
	if appID == "" {
		return nil
	}

	m.logger.Debugf("刷新%s的mDNS地址.", appID)

	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()

	if err := m.browse(ctx, appID, nil); err != nil {
		return err
	}

	// 等到上下文被取消或超时。
	<-ctx.Done()

	if errors.Is(ctx.Err(), context.Canceled) {
		// 这不是预期的，请调查为什么context被取消了。
		m.logger.Warnf("取消了刷新%s的mDNS地址 .", appID)
	} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		m.logger.Debugf("刷新%s的mDNS地址超时了", appID)
	}

	return nil
}

// refreshAllApps 将对当前缓存中的每个地址进行mDNS网络浏览。这个函数是阻塞的。
func (m *resolver) refreshAllApps(ctx context.Context) error {
	m.logger.Debug("刷新 all mDNS addresses.")

	// 检查我们的地址缓存中是否有任何需要刷新的IPv4或IPv6地址。
	m.ipv4Mu.RLock()
	numAppIPv4Addr := len(m.appAddressesIPv4)
	m.ipv4Mu.RUnlock()

	m.ipv6Mu.RLock()
	numAppIPv6Addr := len(m.appAddressesIPv6)
	m.ipv6Mu.RUnlock()

	numApps := numAppIPv4Addr + numAppIPv6Addr
	if numApps == 0 {
		m.logger.Debug("no mDNS apps to refresh.")

		return nil
	}

	var wg sync.WaitGroup

	// 过期的地址将被驱逐  getAppIDs()
	for _, appID := range m.getAppIDs() {
		wg.Add(1)

		go func(a string) {
			defer wg.Done()

			err := m.refreshApp(ctx, a)
			if err != nil {
				return
			}
		}(appID)
	}

	// 等待所有应用刷新完成
	wg.Wait()

	return nil
}

// browse 将对所提供的应用程序ID进行无阻塞的mdns网络浏览。
func (m *resolver) browse(ctx context.Context, appID string, onEach func(ip string)) error {
	//todo https://github.com/ls-2018/mdns
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return fmt.Errorf("未能初始化解析器: %e", err)
	}
	entries := make(chan *zeroconf.ServiceEntry)

	// 处理从mDNS浏览返回的每个服务条目。
	go func(results <-chan *zeroconf.ServiceEntry) {
		//处理返回每一条结果
		for {
			select {
			case <-ctx.Done():
				if errors.Is(ctx.Err(), context.Canceled) {
					m.logger.Debugf("应用%s的mDNS查找取消了.", appID)
				} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					m.logger.Debugf("应用%s的mDNS查找超时了.", appID)
				}
				return
			case entry := <-results:
				if entry == nil {
					break
				}
				for _, text := range entry.Text {
					if text != appID {
						m.logger.Debugf("mDNS响应没有匹配到应用 id %s, 跳过.", appID)
						break
					}
					m.logger.Debugf("mDNS 响应匹配到了应用ID %s.", appID)
					hasIPv4Address := len(entry.AddrIPv4) > 0
					hasIPv6Address := len(entry.AddrIPv6) > 0
					if !hasIPv4Address && !hasIPv6Address {
						m.logger.Debugf("应用 %s的mDNS 响应不包含任何ipv4|ipv6地址，跳过", appID)
						break
					}

					var addr string
					port := entry.Port

					// TODO: 我们目前只使用第一个IPv4和IPv6地址。
					// 我们应该了解在哪些情况下会有额外的地址 以及我们是否需要支持它们。
					if hasIPv4Address {
						addr = fmt.Sprintf("%s:%d", entry.AddrIPv4[0].String(), port)
						m.addAppAddressIPv4(appID, addr)
					}
					if hasIPv6Address {
						addr = fmt.Sprintf("%s:%d", entry.AddrIPv6[0].String(), port)
						m.addAppAddressIPv6(appID, addr)
					}

					if onEach != nil {
						onEach(addr) // 回调函数
					}
				}
			}
		}
	}(entries)
	// 在local.域查找 appID
	if err = resolver.Browse(ctx, appID, "local.", entries); err != nil {
		return fmt.Errorf("浏览失败: %s", err.Error())
	}

	return nil
}

// addAppAddressIPv4 为所提供的应用程序ID添加一个IPv4地址到缓存中。
func (m *resolver) addAppAddressIPv4(appID string, addr string) {
	m.ipv4Mu.Lock()
	defer m.ipv4Mu.Unlock()

	m.logger.Debugf("添加app:%s 实例地址:%s 到ipv4缓存中", addr, appID)
	if _, ok := m.appAddressesIPv4[appID]; !ok {
		var addrList addressList
		m.appAddressesIPv4[appID] = &addrList
	}
	m.appAddressesIPv4[appID].add(addr)
}

// addAppIPv4Address 为所提供的应用程序ID添加一个IPv6地址到缓存中。
func (m *resolver) addAppAddressIPv6(appID string, addr string) {
	m.ipv6Mu.Lock()
	defer m.ipv6Mu.Unlock()

	m.logger.Debugf("添加app:%s 实例地址:%s 到ipv6缓存中", addr, appID)
	if _, ok := m.appAddressesIPv6[appID]; !ok {
		var addrList addressList
		m.appAddressesIPv6[appID] = &addrList
	}
	m.appAddressesIPv6[appID].add(addr)
}

// getAppIDsIPv4 返回一个当前IPv4应用程序ID的列表。该方法使用expire on read来驱逐过期的地址。
func (m *resolver) getAppIDsIPv4() []string {
	m.ipv4Mu.RLock()
	defer m.ipv4Mu.RUnlock()

	appIDs := make([]string, 0, len(m.appAddressesIPv4))
	for appID, addrList := range m.appAddressesIPv4 {
		old := len(addrList.addresses)
		addrList.expire() // 驱逐过期的节点
		m.logger.Debugf("%d IPv4 地址被驱逐 app id %s.", old-len(addrList.addresses), appID)
		appIDs = append(appIDs, appID)
	}

	return appIDs
}

// getAppIDsIPv6 返回一个当前IPv6应用程序ID的列表。该方法使用expire on read来驱逐过期的地址。
func (m *resolver) getAppIDsIPv6() []string {
	m.ipv6Mu.RLock()
	defer m.ipv6Mu.RUnlock()

	appIDs := make([]string, 0, len(m.appAddressesIPv6))
	for appID, addrList := range m.appAddressesIPv6 {
		old := len(addrList.addresses)
		addrList.expire()
		m.logger.Debugf("%d IPv6 地址被驱逐 app id %s.", old-len(addrList.addresses), appID)
		appIDs = append(appIDs, appID)
	}

	return appIDs
}

// getAppIDs 返回当前在缓存中的应用ID的列表，确保过期的地址被驱逐出去。
func (m *resolver) getAppIDs() []string {
	return union(m.getAppIDsIPv4(), m.getAppIDsIPv6())
}

// nextIPv4Address 从缓存中返回所提供的应用程序ID的下一个IPv4地址。
func (m *resolver) nextIPv4Address(appID string) *string {
	m.ipv4Mu.RLock()
	defer m.ipv4Mu.RUnlock()
	addrList, exists := m.appAddressesIPv4[appID]
	if exists {
		addr := addrList.next()
		if addr != nil {
			m.logger.Debugf("再缓存中发现 mDNS IPv4 地址: %s", *addr)

			return addr
		}
	}

	return nil
}

// nextIPv6Address 从缓存中返回所提供的应用程序ID的下一个IPv6地址。
func (m *resolver) nextIPv6Address(appID string) *string {
	m.ipv6Mu.RLock()
	defer m.ipv6Mu.RUnlock()
	addrList, exists := m.appAddressesIPv6[appID]
	if exists {
		addr := addrList.next()
		if addr != nil {
			m.logger.Debugf("再缓存中发现 mDNS IPv4 地址:  %s", *addr)
			return addr
		}
	}

	return nil
}

// union 将两个列表的元素合并成一个集合
func union(first []string, second []string) []string {
	keys := make(map[string]struct{})
	for _, id := range first {
		keys[id] = struct{}{}
	}
	for _, id := range second {
		keys[id] = struct{}{}
	}
	result := make([]string, 0, len(keys))
	for id := range keys {
		result = append(result, id)
	}

	return result
}
