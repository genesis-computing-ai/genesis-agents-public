
import sys
if sys.version_info[0]==2:
    import six
    from six.moves.urllib import request
    opener = request.build_opener(
        request.ProxyHandler(
            {'http': 'http://brd-customer-hl_8bc15bec-zone-isp_proxy1:vysba3k2a7af@brd.superproxy.io:22225',
            'https': 'http://brd-customer-hl_8bc15bec-zone-isp_proxy1:vysba3k2a7af@brd.superproxy.io:22225'}))
    print(opener.open('http://lumtest.com/myip.json').read())
if sys.version_info[0]==3:
    import urllib.request
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler(
            {'http': 'http://brd-customer-hl_8bc15bec-zone-isp_proxy1:vysba3k2a7af@brd.superproxy.io:22225',
            'https': 'http://brd-customer-hl_8bc15bec-zone-isp_proxy1:vysba3k2a7af@brd.superproxy.io:22225'}))
    print(opener.open('http://lumtest.com/myip.json').read())