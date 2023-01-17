# -*- coding: utf-8 -*-
"""
Created on Mon Jun 14 09:58:34 2021

@author: Dennis Hou
"""

import pandas as pd


class STPEvent(object):

    @staticmethod
    def motor_start_event(motor_speed, speed=None):
        """
        Capture motor start event from motor speed signal

        Parameters
        ----------
        motor_speed is panda array, with timestamp index
        Returns
        -------
        start_event
        each event is in form of tuple, 1st element is the timestamp at 0 rpm, 2nd is the timestamp of start
        return None if there is no event
        """
        start_event = []
        begin = None
        for i in range(len(motor_speed.index)):
            if speed:
                if not begin and motor_speed[motor_speed.index[i]] == 0:
                    begin = motor_speed.index[i]
                if begin and motor_speed[motor_speed.index[i]] >= speed:
                    end = motor_speed.index[i]
                    start_event.append((begin, end))
                    begin = None
            else:
                if motor_speed[motor_speed.index[i]] == 0 and motor_speed[motor_speed.index[i+1]] != 0:
                    start_event.append((motor_speed.index[i], motor_speed.index[i + 1]))
        if len(start_event) == 0:
            start_event = None

        return start_event

    @staticmethod
    def motor_stop_event(motor_speed, speed=None):
        """
        Capture motor stop event from motor speed signal

        Parameters
        ----------
        motor_speed is panda array, with timestamp index
        Returns
        -------
        stop_event
        each event is in form of tuple, 1st element is the timestamp of stop, 2nd is the timestamp of start
        return None if there is no event
        """
        stop_event = []
        begin = None
        for i in range(len(motor_speed.index)-1):
            if speed:
                if not begin and motor_speed[motor_speed.index[i]] >= speed:
                    begin = motor_speed.index[i]
                if begin and motor_speed[motor_speed.index[i]] == 0:
                    end = motor_speed.index[i]
                    stop_event.append((begin, end))
                    begin = None
            else:
                if motor_speed[motor_speed.index[i]] != 0 and motor_speed[motor_speed.index[i+1]] == 0:
                    stop_event.append((motor_speed.index[i], motor_speed.index[i + 1]))
        if len(stop_event) == 0:
            stop_event = None

        return stop_event

    @staticmethod
    def pump_swap_event(pump_hour):
        """
        Parameters
        ----------
        pump_hour
        pump_hour is panda array, with timestamp index
        Returns
        -------
        swap event
        each event is in form of tuple, 1st element is the timestamp before swap, 2nd is the timestamp after swap
        return None if there is no event
        """
        swap_event = []
        for i in range(len(pump_hour.index)-1):
            if pump_hour[pump_hour.index[i+1]] < pump_hour[pump_hour.index[i]]:
                swap_event.append((pump_hour.index[i], pump_hour.index[i + 1]))
        if len(swap_event) == 0:
            swap_event = None

        return swap_event
