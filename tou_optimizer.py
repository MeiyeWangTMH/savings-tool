#!/usr/local/bin/python

import fm_opt_simul as simul
from scipy.optimize import minimize
import numpy as np

def main(fname,Folder,kW_max,desired_fulfillment,hard_max,rate_dict,season,days):

    def optimize(x0):
        # print(x0)

        def objective(x):

            user_filename = Folder + '/' + fname

            a_result = calcGridmax(x)


            charged_power = a_result[2]
            # print('X after simul.main')
            # print(charged_power)

            # print(charged_power)
            site_load = a_result[3]
            # print(site_load)
            total_load = [x + y for x, y in zip(charged_power, site_load)]
            # print(total_load)
            max_kW = max(total_load) #only used to determine which rate to use if multiple were input

            # set default  costs as 0
            energy_rate = [0 for i in range(onedaycount)]
            demand_cost = 0
            customer_charge = 0

            if isinstance(sorted_rate_dict, list):
                max_kW = max(total_load)
                min_kw_restrictions = []
                for i in sorted_rate_dict:
                    min_kw_restrictions.append(i['Rate kW Minimum Restriction'].min())
                min_kw_restrictions.sort()
                min_kW = 0
                for n in min_kw_restrictions:
                    if max_kW > n:
                        min_kW = n
                for i in sorted_rate_dict:
                    if min_kW == i['Rate kW Minimum Restriction'].min():
                        temp_dict = i
            else:
                temp_dict = sorted_rate_dict

            for index, values in temp_dict.iterrows():

                start = int(values['Start Interval'] - 1)
                end = int(values['End Interval'] - 1)
                # print(start)
                # print(end)
                # print(total_load)
                if end == -1:
                    end = 96
                if start == -1:
                    start = 0

                energy = values['Energy Charges\n($/kWh)']
                demand = values['Demand Charges\n($/kW)']
                customer_charge = values['Customer Charges\n($ / Month - per meter)']


                if start < end:
                    energy_rate[start:end] = [energy] * len(energy_rate[start:end])
                    max_demand = max(max(total_load[start:end]), max(total_load[start + 96:end + 96]))
                    demand_cost += max_demand * demand
                else:
                    energy_rate[start:] = [energy] * len(energy_rate[start:])
                    energy_rate[:end] = [energy] * len(energy_rate[:end])
                    d1_max = max(max(total_load[start:end + 96]), max(total_load[:end]))
                    d2_max = max(max(total_load[start + 96:]), max(total_load[96:end + 96]))
                    max_demand = max(d1_max, d2_max)
                    demand_cost += max_demand * demand
            full_energy_rate = energy_rate + energy_rate
            charged_energy = [a / 4 for a in charged_power]
            # print("energy rate for managed charging")
            # print(full_energy_rate)
            a_energy_cost = [a * b for a, b in zip(full_energy_rate, charged_energy)]

            # the array is for 2 days, we will assume a 30 day month ; to ignore weekends reduce to 11 (from 15)
            '''This will eventually be handled in input file'''

            energy_cost = sum(a_energy_cost) * days

            # Calculate max demand/facilities related demand
            '''The below also takes into account block rates
                Can be changed to read from tariff dictionary if we see a lot of rates like this, right now we only have PG&E'''
            block = 0
            if temp_dict['Tariff Name'].unique()[0] == 'BEV-1':
                block = 10
            elif temp_dict['Tariff Name'].unique()[0] == 'BEV-2-S Secondary' or temp_dict['Tariff Name'].unique()[0] == 'BEV-2-P Primary':
                block = 50

            if block == 0:
                max_cost = max_kW * temp_dict['Max Demand\n($/kW)'].max()
                if np.isnan(max_cost):
                    max_cost = 0
            else:
                subscription = max_kW // block
                overage = max_kW % block
                max_rate = temp_dict['Max Demand\n($/kW)'].max()
                overage_rate = temp_dict['Overage fee ($/kW)'].max()

                peak_charge_1 = (subscription * block * max_rate) + (overage * overage_rate)
                peak_charge_2 = ((subscription * block) + block) * max_rate

                max_cost = min(peak_charge_1, peak_charge_2)

            # print('total energy cost ' + str(round(energy_cost,2)) + ' total demand cost ' + str(round(demand_cost,2)))


            # print('total customer charge ' + str(customer_charge))
            # print('total max cost ' + str(max_cost))
            return energy_cost + demand_cost + customer_charge + max_cost , max_kW

        def calcGridmax(x):
            a_gridmax = spreadGridmax(x)
            # print('X0 in a vector')
            # print(a_gridmax)
            a_result = simul.main(fname,a_gridmax,0,Folder,season)

            return a_result


        def calcFulfillment(x):
            a_gridmax = spreadGridmax(x)

            a_result = simul.main(fname,a_gridmax,0,Folder,season)

            # print(a_result)

            return a_result[1]

        def spreadGridmax(x):
            temp_gridmax = [0] * onedaycount

            if isinstance(sorted_rate_dict, list):
                max_kW = max(x)
                min_kw_restrictions = []
                for i in sorted_rate_dict:
                    min_kw_restrictions.append(i['Rate kW Minimum Restriction'].min())
                min_kw_restrictions.sort()
                min_kW = 0
                for n in min_kw_restrictions:
                    if max_kW > n:
                        min_kW = n
                for i in sorted_rate_dict:
                    if min_kW == i['Rate kW Minimum Restriction'].min():
                        temp_dict = i
            else:
                temp_dict = sorted_rate_dict

            for i in range(len(temp_gridmax)):
                for index, values in temp_dict.iterrows():
                    start = int(values['Start Interval'] - 1)
                    # end = int(values['End Interval'] - 1)
                    end = int(values['End Interval'])
                    location = temp_dict.index.get_loc(index)
                    if start < end:
                        if start <= i < end:
                            temp_gridmax[i] = x[location]
                    elif end > i and (i + 96) >= start:
                        temp_gridmax[i] = x[location]
                    elif start <= i < (end + 96):
                        temp_gridmax[i] = x[location]
            # print(temp_gridmax)
            a_gridmax = temp_gridmax + temp_gridmax

            return a_gridmax

        def constraint(x):
            desired = desired_fulfillment
            # print(desired_fulfillment)
            # print(calcFulfillment(x))
            return calcFulfillment(x) - desired

        cons = [{'type':'ineq','fun':constraint}]

        # construct the bounds in the form of constraints
        # lower bound must be set at 1, set upper based on hardmax
        if hard_max == 0:
            bounds = [1]
            for factor in range(len(x0)):
                lower = bounds
                l = {'type': 'ineq',
                     'fun': lambda x, lb=lower, i=factor: x[i] - lb}
                cons.append(l)
        else:
            bounds = [1,hard_max]

            for factor in range(len(x0)):
                lower, upper = bounds
                l = {'type': 'ineq',
                     'fun': lambda x, lb=lower, i=factor: x[i] - lb}
                u = {'type': 'ineq',
                     'fun': lambda x, ub=upper, i=factor: ub - x[i]}
                cons.append(l)
                cons.append(u)
        # print(x0)
        sol = minimize(objective,x0,method='COBYLA',constraints=cons,tol=0.01)
        print(x0)
        print(sol.message)
        print(sol.x)
        print(calcFulfillment(sol.x))
        print(objective(sol.x))

        return sol, calcFulfillment(sol.x), objective(sol.x)[0], objective(sol.x)[1]

    #Set up initial rate guesses and TOUs
    global sorted_rate_dict
    # print(rate_dict)
    if isinstance(rate_dict,list): #checks if we are optimizing on multiple rates
        sorted_rate_dict = []
        for i in rate_dict:
            i['Start Interval'] = i['TOU Start - local time\n(incl)'].apply(lambda x: (x.hour + (x.minute / 60)) * 4)
            i['End Interval'] = i['TOU End - local time\n(excl)'].apply(lambda x: (x.hour + (x.minute / 60)) * 4)
            sorted = i.sort_values(['Demand Charges\n($/kW)', 'Energy Charges\n($/kWh)'],ascending=[True, True])
            print(sorted)
            sorted_rate_dict.append(sorted)# sorted_rate_dict has the highest rate first, and lower rates following

    else:
        rate_dict['Start Interval'] = rate_dict['TOU Start - local time\n(incl)'].apply(lambda x: (x.hour + (x.minute / 60)) * 4)
        rate_dict['End Interval'] = rate_dict['TOU End - local time\n(excl)'].apply(lambda x: (x.hour + (x.minute / 60)) * 4)
        sorted_rate_dict = rate_dict.sort_values(['Demand Charges\n($/kW)', 'Energy Charges\n($/kWh)'],ascending=[True, True])
    # print(sorted_rate_dict)

    global onedaycount
    onedaycount = 96

    def initial_guess(sorted_rate_dict):
        # set initial guesses, based on TOU's
        if isinstance(sorted_rate_dict, list):
            min_kw_restrictions = []
            for i in sorted_rate_dict:
                min_kw_restrictions.append(i['Rate kW Minimum Restriction'].min())
            min_kw_restrictions.sort()
            min_kW = 0
            for n in min_kw_restrictions:
                if n < kW_max :
                    min_kW = n
            for i in sorted_rate_dict:
                if min_kW == i['Rate kW Minimum Restriction'].min():
                    temp_dict_tou = i

        if isinstance(sorted_rate_dict, list):
            x0 = np.array([0] * len(temp_dict_tou))
            print('setting based on ' + str(len(temp_dict_tou)) + ' tous')
        else:
            # does this need to be updated to set on tou's vs separete times (e.g. 4 if 2 part peaks vs 3)
            x0 = np.array([0] * len(sorted_rate_dict))

        x0[0] = kW_max  # set offpeak guess at user input max for first guess

        for i in range(1, len(x0) - 1):
            x0[i] = kW_max

        return x0

    ####USER INPUT
    '''Set initial grid max array based on rate and/or desired optimization type
        e.g. if you want to shift all charging to cheapest TOU, then enter nothing for the other values below
        e.g. if you want to keep the load flat, then set all other values in array as the same as x0[0]
        e.g. set custom array based on known loads from past runs
        etc.
        This will eventually be changed to be part of input file'''
    #

    x0 = initial_guess(sorted_rate_dict)

    # for i in range(1,len(x0)):
    #     x0[i] = kW_max
    # x0[1] = kW_max/2
    # x0[2] = kW_max/10
    # x0[3] = kW_max/2

    '''Define increment'''
    # # increment = 10
    increment = 50


    response, fulfillment, cost, max_demand = optimize(x0)

    multiplier = desired_fulfillment/fulfillment * 1.5

    count = 0
    while response.message != 'Optimization terminated successfully.' or count == 0:
        if count == 0 and response.message == 'Optimization terminated successfully.':
            x0 = x0 / 2 #first guess was too high, start over

        elif fulfillment < desired_fulfillment:
            # x0[0] = x0[0] + increment

            ####USER INPUT
            '''Set the increments below based on rate and/or desired optimization type
                e.g. if you want to keep no charging in the most expensive time, do not add increments
                e.g. if you want to keep the load flat, then set all values in array to increase
                etc.
                This will eventually be changed to be part of input file'''
            #
            # x0[1] = x0[1] + increment
            # x0[2] = x0[2] + increment
            # x0[3] = x0[3] + increment
            for i in range(0,len(x0)):
                x0[i] = x0[i] + increment
                # x0 [i] = x0[i] * multiplier
            # for i in range(0,len(x0)-1):
            #     x0[i] = x0[i] + increment

        elif fulfillment > desired_fulfillment -1:
            x0 = response.x

        # print(x0)

        #check if max in x0 indicates new rate, if so, check if # tou's have changed, if so reset x0 to correct # tous
        if isinstance(sorted_rate_dict, list):
            max_kW = max_demand + increment
            min_kw_restrictions = []
            for i in sorted_rate_dict:
                min_kw_restrictions.append(i['Rate kW Minimum Restriction'].min())
            min_kw_restrictions.sort()
            min_kW = 0
            for n in min_kw_restrictions:
                if max_kW > n:
                    min_kW = n
            for i in sorted_rate_dict:
                if min_kW == i['Rate kW Minimum Restriction'].min():
                    temp_dict_tou = i
            if len(temp_dict_tou) != len(x0):
                print('setting based on ' + str(len(temp_dict_tou)) + 'tous')
                y = np.array([0] * len(temp_dict_tou))
                y[0] = max_kW
                y[1] = x0[len(x0)-1]
                x0 = y

        response, fulfillment,cost, max_demand = optimize(x0)

        count += 1
        if count >= 300:
            print("Attempted Optimization 100 times, start over with different GridMax values")
            break

    if count <2 :
        print ('Initial guess too high, recommend running with lower input guess')
    tou_array = response.x

    temp_gridmax = [0] * onedaycount

    if isinstance(sorted_rate_dict, list):
        max_kW = max_demand
        min_kw_restrictions = []
        for i in sorted_rate_dict:
            min_kw_restrictions.append(i['Rate kW Minimum Restriction'].min())
        min_kw_restrictions.sort()
        min_kW = 0
        for n in min_kw_restrictions:
            if max_kW > n:
                min_kW = n
        for i in sorted_rate_dict:
            if min_kW == i['Rate kW Minimum Restriction'].min():
                temp_dict = i
        sorted_rate_dict = temp_dict
        print('used rate ' + str(sorted_rate_dict['Tariff Name'].unique()[0]) + 'for max ' + str(max_kW))

    for i in range(len(temp_gridmax)):
        for index,values in sorted_rate_dict.iterrows():
            # start = int(values['Start Interval'] - 1)
            # end = int(values['End Interval'] - 1)
            start = int(values['Start Interval'])
            end = int(values['End Interval'])
            location = sorted_rate_dict.index.get_loc(index)
            if start < end:
                if start <= i < end:
                    temp_gridmax[i] = tou_array[location]
            elif end > i  and (i+96) >= start:
                temp_gridmax[i] = tou_array[location]
            elif start <= i < (end + 96):
                temp_gridmax[i] = tou_array[location]

    a_gridmax = temp_gridmax + temp_gridmax
    a_result = simul.main(fname, a_gridmax, 1,Folder,season)
    a_result.append(sorted_rate_dict['Tariff Name'].unique()[0])

    return response.x, cost, a_result