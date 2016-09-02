import requests
from concord.utils import (get_config, build_logger)
from thrift import TSerialization
from zookeeper_utils import get_scheduler_master_url
from dcos_utils import * 
from dcos import http # http.request
from concord.internal.thrift.ttypes import (
    BoltComputationRequest,
    TopologyMetadata
)

logger = build_logger('cmd.http_utils')

def _http_request_url(slash_command):
    # if ON_DCOS: blah blah
    url = get_scheduler_master_url(get_config().get('zookeeper_hosts'),
                                   get_config().get('zookeeper_path'))
    if url is None:
        logger.critical("Failed when retrieving url from zk")
        raise Exception("Could not retrieve master schedulers url")
    return url.rstrip('/') + '/concord' # Get svc name

def _http_request_json(method, slash_command, **kwargs):
    request_url = _http_request_url(slash_command)
    logger.info("Attemping request to scheduler at: %s" % request_url)
    try:
        if ON_DCOS is True:
            # On 401 will make request with dcos auth
            response = http.request(method, request_url, headers={},
                                    params=kwargs.get('params'), data=kwargs.get('data'))
        else:
            response = requests.request(method, request_url, headers={},
                                        params=kwargs.get('params'), data=kwargs.get('data'))
        response.raise_for_status()
        json_obj = response.json()
        return json_obj
    except Exception as e:
        logger.critical("Error occurred during request/response from %s" % request_url)
        raise e
        
def request_topology_map():
    # Returns: TopologyMetadata structure
    return deserialize(_http_request_json('get', '/concord'))

def request_exists_operator(operator_id):
    return _http_request_json('get', '/concord', { 'id' : operator_id })

def request_create_operator(bolt_computation_request):
    return _http_request_json('post', '/concord', data=serialize(bolt_computation_request))

def request_delete_operator(operator_id):
    return _http_request_json('delete', '/concord', { 'id' : operator_id })

def request_delete_all():
    return _http_request_json('delete', '/concord')
        
