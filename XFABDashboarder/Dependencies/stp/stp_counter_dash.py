# -*- coding: utf-8 -*-
"""
Created on Mon Nov  1 10:05:58 2021

@author: Administrator
"""

import pickle
import matplotlib.pyplot as plt
from os import path

class StpCounterDash(object):
    def __init__(self, data, tool_name, des_path='./'):
        self.data = data.sort_index()
        self.tool_name = tool_name
        self.des_path = des_path
        
        
    def plot_counter(self):
        try:
            assert 'Pump Hour Counter' in self.data.columns
            pump_hour_counter = self.data['Pump Hour Counter'].dropna()
        except AssertionError:
            pump_hour_counter = []
        try:
            assert 'Controller Hour Counter' in self.data.columns
            ctlr_hour_counter = self.data['Controller Hour Counter'].dropna()
        except AssertionError:
            ctlr_hour_counter = []
        try:
            assert 'Start Counter' in self.data.columns
            start_counter = self.data['Start Counter'].dropna()
        except AssertionError:
            start_counter = []
        try:
            assert 'Damage Limit Counter' in self.data.columns
            damage_limit_counter = self.data['Damage Limit Counter'].dropna()
        except AssertionError:
            damage_limit_counter = []
        try:
            assert 'Disturbance Counter' in self.data.columns        
            disturbance_counter = self.data['Disturbance Counter'].dropna()
        except AssertionError:
            disturbance_counter = []
        try:
            assert 'PF Counter' in self.data.columns
            pf_counter = self.data['PF Counter'].dropna()
        except AssertionError:
            pf_counter = []
        
        fig, axes = plt.subplots(2, 3, sharex=True, squeeze=True, figsize=(16, 9))
        fig.suptitle(self.tool_name, y=0.95)
        
        axes[0][0].plot(pump_hour_counter, label='Pump Hour Counter')
        axes[0][0].plot(ctlr_hour_counter, label='Controller Hour Counter')
        axes[0][0].set_ylabel('Hours')
        axes[0][0].grid()
        axes[0][0].legend()
        
        axes[0][1].plot(start_counter)
        axes[0][1].set_ylabel('Start Counter')
        axes[0][1].grid()
        
        axes[0][2].plot(damage_limit_counter)
        axes[0][2].set_ylabel('Damage Limit Counter')
        axes[0][2].grid()
        
        axes[1][0].plot(disturbance_counter)
        axes[1][0].set_ylabel('Disturbance Counter')
        axes[1][0].grid()
        
        axes[1][1].plot(pf_counter)
        axes[1][1].set_ylabel('PF Counter')
        axes[1][1].grid()
        
        _ = plt.setp(axes[1][0].xaxis.get_majorticklabels(), rotation=25)
        _ = plt.setp(axes[1][1].xaxis.get_majorticklabels(), rotation=25)
        _ = plt.setp(axes[1][2].xaxis.get_majorticklabels(), rotation=25)
        
        return fig
    
    def save_plot(self):
        """
        To save the dash into destination folder

        :return:
        """
        
        fig = self.plot_counter()
        
        with open(path.join(self.des_path, '{}.pkl'.format(self.tool_name)), 'wb') as fid:
            pickle.dump(fig, fid)
        fig.savefig(path.join(self.des_path, '{}.png'.format(self.tool_name)))