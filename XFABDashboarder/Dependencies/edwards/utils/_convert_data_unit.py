

def convert_data_unit(data, parameter_number, parameter_unit_id):
    """Convert data unit to the one used by EdCentra."""
    parameter_unit_id_new = parameter_unit_id
    if parameter_unit_id == 20:  # Power W => kW
        data = data / 1000
        parameter_unit_id_new = -1
    elif parameter_unit_id == 21:  # Pressure
        if parameter_number == 46:  # Pa => kPa (Water Pressure)
            data = data * 0.001
            parameter_unit_id_new = -1
        else:  # Pa => psi (Nozzle / Exhaust)
            data = data * 0.0001450377
            parameter_unit_id_new = -1
    elif parameter_unit_id == 24:  # Temperature K => C
        data = data - 273.15
        parameter_unit_id_new = -1
    elif parameter_unit_id == 14:  # Flow m3/s => liter/min
        data = data * 60000
        parameter_unit_id_new = -1
    return data, parameter_unit_id_new
