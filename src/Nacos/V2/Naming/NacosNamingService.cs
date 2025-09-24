namespace Nacos.V2.Naming
{
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Options;
    using Nacos.V2.Common;
    using Nacos.V2.Naming.Cache;
    using Nacos.V2.Naming.Core;
    using Nacos.V2.Naming.Dtos;
    using Nacos.V2.Naming.Event;
    using Nacos.V2.Naming.Remote;
    using Nacos.V2.Remote;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net.Http;
    using System.Threading.Tasks;

    public class NacosNamingService : INacosNamingService
    {
        private static readonly string UP = "UP";
        private static readonly string DOWN = "DOWN";

        private readonly ILogger _logger;
        private readonly NacosSdkOptions _options;

        private string _namespace;

        private ServiceInfoHolder _serviceInfoHolder;

        private InstancesChangeNotifier _changeNotifier;

        private INamingClientProxy _clientProxy;

        public NacosNamingService(
            ILoggerFactory loggerFactory,
            IOptions<NacosSdkOptions> optionAccs,
            IHttpClientFactory clientFactory)
        {
            _logger = loggerFactory.CreateLogger<NacosNamingService>();
            _options = optionAccs.Value;
            _namespace = string.IsNullOrWhiteSpace(_options.Namespace) ? Utils.UtilAndComs.DEFAULT_NAMESPACE_ID : _options.Namespace;
            this._changeNotifier = new InstancesChangeNotifier();
            this._serviceInfoHolder = new ServiceInfoHolder(_logger, _namespace, _options, _changeNotifier);
            this._clientProxy = new NamingClientProxyDelegate(_logger, _namespace, _serviceInfoHolder, _options, _changeNotifier, clientFactory);
        }

        public Task DeregisterInstance(string serviceName, string ip, int port)
            => DeregisterInstance(serviceName, ip, port, Constants.DEFAULT_CLUSTER_NAME);

        public Task DeregisterInstance(string serviceName, string groupName, string ip, int port)
            => DeregisterInstance(serviceName, groupName, ip, port, Constants.DEFAULT_CLUSTER_NAME);

        public Task DeregisterInstance(string serviceName, string ip, int port, string clusterName)
            => DeregisterInstance(serviceName, Constants.DEFAULT_GROUP, ip, port, clusterName);

        public Task DeregisterInstance(string serviceName, string groupName, string ip, int port, string clusterName)
        {
            var instance = new Instance()
            {
                Ip = ip,
                Port = port,
                ClusterName = clusterName
            };

            return DeregisterInstance(serviceName, groupName, instance);
        }

        public Task DeregisterInstance(string serviceName, Instance instance)
            => DeregisterInstance(serviceName, Constants.DEFAULT_GROUP, instance);

        public Task DeregisterInstance(string serviceName, string groupName, Instance instance)
            => _clientProxy.DeregisterService(serviceName, groupName, instance);

        public Task<List<Instance>> GetAllInstances(string serviceName)
            => GetAllInstances(serviceName, new List<string>());

        public Task<List<Instance>> GetAllInstances(string serviceName, string groupName)
            => GetAllInstances(serviceName, groupName, new List<string>());

        public Task<List<Instance>> GetAllInstances(string serviceName, bool subscribe)
            => GetAllInstances(serviceName, new List<string>(), subscribe);

        public Task<List<Instance>> GetAllInstances(string serviceName, string groupName, bool subscribe)
            => GetAllInstances(serviceName, groupName, new List<string>(), subscribe);

        public Task<List<Instance>> GetAllInstances(string serviceName, List<string> clusters)
            => GetAllInstances(serviceName, Constants.DEFAULT_GROUP, clusters, true);

        public Task<List<Instance>> GetAllInstances(string serviceName, string groupName, List<string> clusters)
             => GetAllInstances(serviceName, groupName, clusters, true);

        public Task<List<Instance>> GetAllInstances(string serviceName, List<string> clusters, bool subscribe)
            => GetAllInstances(serviceName, Constants.DEFAULT_GROUP, clusters, subscribe);

        public async Task<List<Instance>> GetAllInstances(string serviceName, string groupName, List<string> clusters, bool subscribe)
        {
            ServiceInfo serviceInfo;
            string clusterString = string.Join(",", clusters);
            if (subscribe)
            {
                serviceInfo = _serviceInfoHolder.GetServiceInfo(serviceName, groupName, clusterString);
                if (serviceInfo == null || !await _clientProxy.IsSubscribed(serviceName, groupName, clusterString).ConfigureAwait(false))
                {
                    serviceInfo = await _clientProxy.Subscribe(serviceName, groupName, clusterString).ConfigureAwait(false);
                }
            }
            else
            {
                serviceInfo = await _clientProxy.QueryInstancesOfService(serviceName, groupName, clusterString, 0, false).ConfigureAwait(false);
            }

            List<Instance> list = serviceInfo.Hosts;
            if (serviceInfo == null || serviceInfo.Hosts == null || !serviceInfo.Hosts.Any())
            {
                return new List<Instance>();
            }

            return list;
        }

        public Task<string> GetServerStatus()
            => Task.FromResult(_clientProxy.ServerHealthy() ? UP : DOWN);

        public Task<ListView<string>> GetServicesOfServer(int pageNo, int pageSize)
            => GetServicesOfServer(pageNo, pageSize, Constants.DEFAULT_GROUP);

        public Task<ListView<string>> GetServicesOfServer(int pageNo, int pageSize, string groupName)
            => GetServicesOfServer(pageNo, pageSize, groupName, null);

        public Task<ListView<string>> GetServicesOfServer(int pageNo, int pageSize, AbstractSelector selector)
            => GetServicesOfServer(pageNo, pageSize, Constants.DEFAULT_GROUP, selector);

        public Task<ListView<string>> GetServicesOfServer(int pageNo, int pageSize, string groupName, AbstractSelector selector)
            => _clientProxy.GetServiceList(pageNo, pageSize, groupName, selector);

        public Task<List<ServiceInfo>> GetSubscribeServices()
            => Task.FromResult(_changeNotifier.GetSubscribeServices());

        public Task RegisterInstance(string serviceName, string ip, int port)
            => RegisterInstance(serviceName, ip, port, Constants.DEFAULT_CLUSTER_NAME);

        public Task RegisterInstance(string serviceName, string groupName, string ip, int port)
            => RegisterInstance(serviceName, groupName, ip, port, Constants.DEFAULT_CLUSTER_NAME);

        public Task RegisterInstance(string serviceName, string ip, int port, string clusterName)
            => RegisterInstance(serviceName, Constants.DEFAULT_GROUP, ip, port, clusterName);

        public Task RegisterInstance(string serviceName, string groupName, string ip, int port, string clusterName)
        {
            var instance = new Instance()
            {
                Ip = ip,
                Port = port,
                Weight = 1.0d,
                ClusterName = clusterName
            };

            return RegisterInstance(serviceName, groupName, instance);
        }

        public Task RegisterInstance(string serviceName, Instance instance)
            => RegisterInstance(serviceName, Constants.DEFAULT_GROUP, instance);

        public Task RegisterInstance(string serviceName, string groupName, Instance instance)
            => _clientProxy.RegisterServiceAsync(serviceName, groupName, instance);

        public Task<List<Instance>> SelectInstances(string serviceName, bool healthy)
            => SelectInstances(serviceName, new List<string>(), healthy);

        public Task<List<Instance>> SelectInstances(string serviceName, string groupName, bool healthy)
            => SelectInstances(serviceName, groupName, healthy, true);

        public Task<List<Instance>> SelectInstances(string serviceName, bool healthy, bool subscribe)
            => SelectInstances(serviceName, new List<string>(), healthy, subscribe);

        public Task<List<Instance>> SelectInstances(string serviceName, string groupName, bool healthy, bool subscribe)
            => SelectInstances(serviceName, groupName, new List<string>(), healthy, subscribe);

        public Task<List<Instance>> SelectInstances(string serviceName, List<string> clusters, bool healthy)
            => SelectInstances(serviceName, clusters, healthy, true);

        public Task<List<Instance>> SelectInstances(string serviceName, string groupName, List<string> clusters, bool healthy)
            => SelectInstances(serviceName, groupName, clusters, healthy, true);

        public Task<List<Instance>> SelectInstances(string serviceName, List<string> clusters, bool healthy, bool subscribe)
            => SelectInstances(serviceName, Constants.DEFAULT_GROUP, clusters, healthy, subscribe);

        public async Task<List<Instance>> SelectInstances(string serviceName, string groupName, List<string> clusters, bool healthy, bool subscribe)
        {
            ServiceInfo serviceInfo;
            string clusterString = string.Join(",", clusters);
            if (subscribe)
            {
                serviceInfo = _serviceInfoHolder.GetServiceInfo(serviceName, groupName, clusterString);
                if (serviceInfo == null)
                {
                    serviceInfo = await _clientProxy.Subscribe(serviceName, groupName, clusterString).ConfigureAwait(false);
                }
            }
            else
            {
                serviceInfo = await _clientProxy.QueryInstancesOfService(serviceName, groupName, clusterString, 0, false).ConfigureAwait(false);
            }

            return SelectInstances(serviceInfo, healthy);
        }

        private List<Instance> SelectInstances(ServiceInfo serviceInfo, bool healthy)
        {
            List<Instance> list = serviceInfo.Hosts;

            if (serviceInfo == null || list == null || !list.Any()) return new List<Instance>();

            return list.Where(x => x.Healthy.Equals(healthy) && x.Enabled && x.Weight > 0).ToList();
        }

        public Task<Instance> SelectOneHealthyInstance(string serviceName)
            => SelectOneHealthyInstance(serviceName, new List<string>());

        public Task<Instance> SelectOneHealthyInstance(string serviceName, string groupName)
            => SelectOneHealthyInstance(serviceName, groupName, true);

        public Task<Instance> SelectOneHealthyInstance(string serviceName, bool subscribe)
            => SelectOneHealthyInstance(serviceName, new List<string>(), subscribe);

        public Task<Instance> SelectOneHealthyInstance(string serviceName, string groupName, bool subscribe)
            => SelectOneHealthyInstance(serviceName, groupName, new List<string>(), subscribe);

        public Task<Instance> SelectOneHealthyInstance(string serviceName, List<string> clusters)
            => SelectOneHealthyInstance(serviceName, clusters, true);

        public Task<Instance> SelectOneHealthyInstance(string serviceName, string groupName, List<string> clusters)
            => SelectOneHealthyInstance(serviceName, groupName, clusters, true);

        public Task<Instance> SelectOneHealthyInstance(string serviceName, List<string> clusters, bool subscribe)
            => SelectOneHealthyInstance(serviceName, Constants.DEFAULT_GROUP, clusters, subscribe);

        public async Task<Instance> SelectOneHealthyInstance(string serviceName, string groupName, List<string> clusters, bool subscribe)
        {
            string clusterString = string.Join(",", clusters);
            if (subscribe)
            {
                ServiceInfo serviceInfo = _serviceInfoHolder.GetServiceInfo(serviceName, groupName, clusterString);

                if (serviceInfo == null)
                {
                    serviceInfo = await _clientProxy.Subscribe(serviceName, groupName, clusterString).ConfigureAwait(false);
                }

                return Balancer.GetHostByRandom(serviceInfo?.Hosts);
            }
            else
            {
                ServiceInfo serviceInfo = await _clientProxy
                        .QueryInstancesOfService(serviceName, groupName, clusterString, 0, false).ConfigureAwait(false);

                return Balancer.GetHostByRandom(serviceInfo?.Hosts);
            }
        }

        public Task ShutDown() => Task.CompletedTask;

        public Task Subscribe(string serviceName, IEventListener listener)
            => Subscribe(serviceName, new List<string>(), listener);

        public Task Subscribe(string serviceName, string groupName, IEventListener listener)
            => Subscribe(serviceName, groupName, new List<string>(), listener);

        public Task Subscribe(string serviceName, List<string> clusters, IEventListener listener)
            => Subscribe(serviceName, Constants.DEFAULT_GROUP, clusters, listener);

        public async Task Subscribe(string serviceName, string groupName, List<string> clusters, IEventListener listener)
        {
            if (listener == null) return;

            string clusterString = string.Join(",", clusters);

            _changeNotifier.RegisterListener(groupName, serviceName, clusterString, listener);
            await _clientProxy.Subscribe(serviceName, groupName, clusterString).ConfigureAwait(false);
        }

        public Task Unsubscribe(string serviceName, IEventListener listener)
            => Unsubscribe(serviceName, new List<string>(), listener);

        public Task Unsubscribe(string serviceName, string groupName, IEventListener listener)
            => Unsubscribe(serviceName, groupName, new List<string>(), listener);

        public Task Unsubscribe(string serviceName, List<string> clusters, IEventListener listener)
            => Unsubscribe(serviceName, Constants.DEFAULT_GROUP, clusters, listener);

        public async Task Unsubscribe(string serviceName, string groupName, List<string> clusters, IEventListener listener)
        {
            string clustersString = string.Join(",", clusters);

            _changeNotifier.DeregisterListener(groupName, serviceName, clustersString, listener);
            if (!_changeNotifier.IsSubscribed(groupName, serviceName, clustersString))
            {
                await _clientProxy.Unsubscribe(serviceName, groupName, clustersString).ConfigureAwait(false);
            }
        }

        public Task BatchRegisterInstance(string serviceName, string groupName, List<Instance> instances)
        {
            Naming.Utils.NamingUtils.BatchCheckInstanceIsLegal(instances);

            return _clientProxy.BatchRegisterServiceAsync(serviceName, groupName, instances);
        }
    }
}
