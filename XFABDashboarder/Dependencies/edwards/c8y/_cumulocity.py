# -*- coding: utf-8 -*-
"""
Created on Wed Dec  2 17:52:06 2020

@author: Danny Sun
"""

import warnings

import requests
from base64 import b64encode


class Cumulocity:
    
    ERROR_NOT_FOUND = 404
    
    def __init__(self, tenant_url, username, password): 
        """Initialize `Cumulocity` object.
        
        Parameters
        ----------
        tenant_url : str
        username : str
        password : str
        """
        credentials = b64encode(bytes(username + ':' + \
                                      password, 'utf-8')).decode('ascii')
        self.headers = {'Authorization': 'Basic %s' % credentials}
        end_points = {
            'epManagedObjects'   : '/inventory/managedObjects',
            'epMeasurements'     : '/measurement/measurements',
            'epMeasureSeries'    : '/measurement/measurements/series',
            'epAnalyticsBuilder' : '/service/cep/analyticsbuilder',
            'epZementis'         : '/service/zementis',
            'epEvents'           : '/event/events'}
        self.cfg = {'tenant_url': tenant_url,
                    'username': username,
                    'password': password,
                    'end_points': end_points}

    def modify_zementis_model(self, model_name, active):
        """Activate or deactivate a Zementis model.
        
        Parameters
        ----------
        model_name : str
        active : bool
            True, activate; False, deactivate
        
        Returns
        -------
        bool
        """        
        state = 'activate' if active else 'deactivate'
        url = (self.cfg.get('tenant_url') + \
               self.cfg.get('end_points').get('epZementis') + \
               '/model/' + model_name + '/' + state)
        r = requests.put(url, headers=self.headers)
        return r
    
    def get_all_zementis_model(self, basename = None):
        """Get all zementis models whose name starts with `basename`.
        
        Parameters
        ----------
        basename : str
        
        Returns
        -------
        List of str
        """
        url = (self.cfg.get('tenant_url') + \
               self.cfg.get('end_points').get('epZementis') + '/models')
        r = requests.get(url, headers=self.headers)
        models = r.json().get('models')
        if basename is not None:
            models = [x for x in models if x.find(basename) == 0]
        return models 
    
    def is_device_exist(self, device_name):
        """Whether device exists.
        
        Parameters
        ----------
        device_name : str
        
        Returns
        -------
        bool
        """ 
        url = (self.cfg.get('tenant_url') + \
               self.cfg.get('end_points').get('epManagedObjects')) 
        params = {'fragmentType': 'c8y_IsDevice',
          'query' : 'name eq ' + device_name}
        r = requests.get(url, headers=self.headers, params=params)
        return len(r.json().get('managedObjects')) == 1

    def get_device_id(self, device_name):
        """Get device id.
        
        Parameters
        ----------
        device_name : str
        
        Returns
        -------
        int
        """ 
        url = (self.cfg.get('tenant_url') + \
               self.cfg.get('end_points').get('epManagedObjects')) 
        params = {'fragmentType': 'c8y_IsDevice',
                  'query' : 'name eq ' + device_name}
        r = requests.get(url, headers=self.headers, params=params)
        if len(r.json().get('managedObjects')) == 1:
            return int(r.json().get('managedObjects')[0].get('id', None))
        else :
            warnings.warn((device_name + ' does not exist!'), UserWarning)
            return None

    def get_supported_measurements_series(self, device_id):
        """Get supported measurements and series of a device.
        
        Parameters
        ----------
        device_id : int
        
        Returns
        -------
        list of str
        """ 
        url = (self.cfg.get('tenant_url') + \
               self.cfg.get('end_points').get('epManagedObjects') + \
               '/' + str(device_id) + '/' + 'supportedSeries')
        r = requests.get(url, headers=self.headers)
        return sorted(r.json().get('c8y_SupportedSeries'))