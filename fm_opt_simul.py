# This Python file uses the following encoding: utf-8
import os, sys
 # INFO ZU PROGRAMM - FUNKTIONEN
# needs to be done for Code to run:
# go to File – Settings – Project [name] – Project Interpreter – [+] (oben rechts) -  entry:
# "xlsxwriter"& press install
# & install "pandas"
# & install "xlrd"
# & install "matplotlib"


################################################################################################################
# ----- SCHEDULED LOADMANAGEMENT  -------
# how it works:
# prioritization based on SoC & Charging Power & departure; must-charge is considered
# order: must_charge - prio - already charging - wants to charge
# cars get their full power by order if available

# time buffer for urgency coefficient calculation, the more the buffer is,
 # the sooner the EV becomes a must-charge (1 = 15min)
time_buffer = 1
################################################################################################################


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
import time as timedate
from datetime import datetime,  timedelta
from operator import itemgetter, attrgetter, methodcaller

def main(fname,grid_max,iterate_count,Folder,season):

    twodaycount = 2 * 96
    onedaycount = 96
    a_chargepower_all = [0 for i in range(twodaycount)]

    a_gridmax = grid_max

    # how do you want to sort?
     # 1 = main sort by urg.coef, with mustcharge & already charging
     # 2 = main sort by urg.coef, without mustcharge
     # 3 = main sort by dep.time, with must charge, no already charging
     # 4 = main sort by dep.time, without must charge, no already charging
     # 5 = main sort by dep.time, without must charge & already charging
    how_to_sort = 1

    # setup Class for EV´s
    class Cars:
        def __init__(self):
            self.number = 0
            self.arrival = 0
            self.departure = 0
            self.chargepower = 0
            self.a_plugtime = [0 for i in range(twodaycount)]
            self.chargingstatus = [0 for i in range(twodaycount)]
            self.energydemand = 0
            self.remain_energydemand = 0
            self.obt_chargepower = [0 for i in range(twodaycount)]
            self.final_energydemand = 0
            self.pointsum = 0
            self.phases = 0
            self.min_chpower = 0
            self.obt_ch_eff = [0 for i in range(twodaycount)]
            self.prioritized = 1
            self.chargingevents = 0
            self.urg_coefficient = 0
            self.last_timestep_charging = 1
            self.must_charge = 1
            self.chargingstation = 0

        def urgency_coefficient(self, timing):

            departure = self.departure
            if timing > self.departure:
                departure = self.departure + onedaycount
            if timing > self.departure + onedaycount:
                departure = self.departure + twodaycount

            if self.remain_energydemand > 0:  # and self.departure - timing > 0:
                self.urg_coefficient = (self.chargepower * (departure - timing - time_buffer)/4)/self.remain_energydemand
            else:
                self.urg_coefficient = 1000

        def last_time_charging(self,timing):
            if self.obt_chargepower[timing-1] > 0:
                self.last_timestep_charging = 0
            else:
                self.last_timestep_charging = 1

        def is_must_charge(self, timing):
            if timing >= onedaycount:
                timing -= onedaycount

            if self.remain_energydemand > 0 \
                    and self.remain_energydemand >= (self.chargepower * abs(self.departure - timing - time_buffer) / 4 * charging_efficiency[0]):
                self.must_charge = 0
            else:
                self.must_charge = 1

    # ----import Data from Excel-------
    user_filename = Folder + '/' + fname
    file = user_filename + ".xlsx"
    read_file = pd.ExcelFile(file)
    read_data = pd.read_excel(read_file, sheet_name=1)

    temp_siteload = read_data['Site Load'].tolist()
    a_siteload = temp_siteload + temp_siteload
    a_loadmax = [0 for count in range(twodaycount)]

    for count in range(twodaycount):
        a_loadmax[count] = a_gridmax[count] - a_siteload[count]
        if a_loadmax[count] <= 0:
            a_loadmax[count] = 0
    distributable_kW = a_loadmax.copy()

    sheet = read_file.parse(2) #read input EV

    #Check if a different schedule is used for unmanaged
    if iterate_count == 'unmanaged':
        input_df = pd.read_excel(read_file, sheet_name=0, index_col=0, usecols="A:B")
        unmanaged_tab = input_df.loc['Unmanaged Input Tab'][0]
        if unmanaged_tab == 'Input EV 2':
            sheet = read_file.parse(3)

    # schedule = sheet.fillna(0).iloc[:, 6:].as_matrix()
    schedule = sheet.fillna(0).iloc[:, 6:].to_numpy()


    # --------setup a list of eV´s of the class Cars with length of the excel-------------
    numberofcars = schedule.shape[0]    # shape[1] gives you the number of column count/ [0] for row count
    for sch in schedule:
        if sch[1] == 0:
            numberofcars -= 1

    eV = [Cars() for j in range(numberofcars)]
    charging_efficiency = [row[7] for row in schedule]

    # read in parameters for every car that´s in the excel
    for k in range(numberofcars):
        if not schedule[k][1] == 0:
            eV[k].number = k+1
            if schedule[k][0] == 'x':
                eV[k].prioritized = 0
            eV[k].arrival = 4 * (schedule[k][1].hour + schedule[k][1].minute / 60.0)
            eV[k].departure = 4 * (schedule[k][2].hour + schedule[k][2].minute / 60.0)
            eV[k].energydemand = schedule[k][3]
            eV[k].chargepower = schedule[k][4]
            eV[k].phases = schedule[k][5]
            eV[k].min_chpower = schedule[k][6] * eV[k].phases
        else:
            numberofcars -= 1

    # set up an list of 0 and 1 for plugtime
    for car in eV:
        store_plug = [0 for i in range(onedaycount)]
        for j in range(onedaycount):
            if car.arrival > car.departure:
                if j < car.departure or j >= car.arrival:
                    store_plug[j] = 1
            else:
                if j >= car.arrival and j < car.departure:
                    store_plug[j] = 1
        car.a_plugtime = store_plug.copy() + store_plug.copy()

    # ---------------------- Functions -----------------------
    def calc_chargingevents():
        for count in range(numberofcars):
            event = 0
            #if eV[count].obt_chargepower[onedaycount] > 0:
            #    event = 1
            for time in range(onedaycount+1, twodaycount):
                if eV[count].obt_chargepower[time-1] == 0 and eV[count].obt_chargepower[time] > 0:
                    event += 1
            eV[count].chargingevents = event

    def charging_events():
        counts = 0
        max = 0
        for count in range(numberofcars):
            counts += eV[count].chargingevents
            if eV[count].chargingevents > max:
                max = eV[count].chargingevents
        return counts, max

    def export_data_excel():
        input_df = pd.read_excel(read_file, sheet_name=0, index_col=0, usecols="A:B")
        create_output = input_df.loc['Create Excel Output'][0]

        if create_output == 1:
            if iterate_count == 'unmanaged':
                filename = Folder + '/'+ timedate.strftime("%Y%m%d-%H%M%S") + '_No_Charge_Management_with SoC_Result_' + season + "_"  + fname + '.xlsx'
            else:
                filename = Folder + '/'+  timedate.strftime("%Y%m%d-%H%M%S") + '_Simulated_Charge_Management_with SoC_Result_' + season + "_" + fname + '.xlsx'
            writer = pd.ExcelWriter(filename, engine='xlsxwriter')

            # ------- Sheet Results -------
            exc_evcount = pd.DataFrame(count for count in range(1, numberofcars + 1))
            exc_evcount.to_excel(writer, sheet_name='Results', header=["EV Nr"], index=False)

            exc_rem_energy = pd.DataFrame(eV[count].final_energydemand for count in range(numberofcars))
            exc_rem_energy.to_excel(writer, 'Results', startcol=1, header=["Remaining Energy Demand [kWh]"], index=False,
                                    float_format='%.1f')

            calc_chargingevents()
            count_chargingevents, chargingevents_max = charging_events()

            exc_chargingevents = pd.DataFrame([count_chargingevents])
            exc_chargingevents.to_excel(writer, 'Results', startcol=4, header=["Amount of Charging Events: "],
                                           index=False)
            exc_chargingevents_max = pd.DataFrame([chargingevents_max])
            exc_chargingevents_max.to_excel(writer, 'Results', startcol=5, header=["Maximum of Events per EV: "],
                                        index=False)
            exc_charged_energy_all = pd.DataFrame([charged_energy_all])
            exc_charged_energy_all.to_excel(writer, 'Results', startcol=6, header=["Charged Total Energy in kWh: "],
                                            index=False)

                                           # ------- Sheet Calculation EV´s -------
            show_steps = [count for count in range(twodaycount)]

            exc_show_steps = pd.DataFrame(show_steps)
            exc_show_steps.to_excel(writer, sheet_name='Calculation EV´s', startcol=0, header=["Time [hh:mm]"], index=False)
            # index=False makes python not print the indexes of array
            exc_loadmax = pd.DataFrame(a_loadmax)
            exc_loadmax.to_excel(writer, sheet_name='Calculation EV´s', startcol=1, header=["Loadmax [kW]"], index=False)

            exc_sum_chargepower = pd.DataFrame(a_chargepower_all[count] for count in range(twodaycount))
            exc_sum_chargepower.to_excel(writer, sheet_name='Calculation EV´s', index=False, startcol=2,
                                         header=["Sum of wanted Chargepower [kW]"], float_format='%.1f')
            exc_sum_obt_chargedpower = pd.DataFrame(actual_chargedpower_all[count] for count in range(twodaycount))
            exc_sum_obt_chargedpower.to_excel(writer, sheet_name='Calculation EV´s', index=False, startcol=3,
                                              header=["Sum of obtained Chargepower [kW]"], float_format='%.1f')

            columncount = 4
            for carc in range(numberofcars):
                exc_ev_obtpower = pd.DataFrame(eV[carc].obt_chargepower[count] for count in range(twodaycount))
                exc_ev_obtpower.to_excel(writer, sheet_name='Calculation EV´s', index=False, startcol=columncount,
                                         header=["EV %d Obt Power [kW]" % (carc + 1)], float_format='%.1f')
                columncount += 1

                                           # ------- Sheet Plug time for TOU analysis -------
            show_steps = [count for count in range(twodaycount)]

            exc_show_steps = pd.DataFrame(show_steps)
            exc_show_steps.to_excel(writer, sheet_name='Plug Time EV´s', startcol=0, header=["Time [hh:mm]"], index=False)
            # index=False makes python not print the indexes of array
            exc_loadmax = pd.DataFrame(a_loadmax)
            exc_loadmax.to_excel(writer, sheet_name='Plug Time EV´s', startcol=1, header=["Loadmax [kW]"], index=False)

            exc_sum_chargepower = pd.DataFrame(a_chargepower_all[count] for count in range(twodaycount))
            exc_sum_chargepower.to_excel(writer, sheet_name='Plug Time EV´s', index=False, startcol=2,
                                         header=["Sum of wanted Chargepower [kW]"], float_format='%.1f')
            exc_sum_obt_chargedpower = pd.DataFrame(actual_chargedpower_all[count] for count in range(twodaycount))
            exc_sum_obt_chargedpower.to_excel(writer, sheet_name='Plug Time EV´s', index=False, startcol=3,
                                              header=["Sum of obtained Chargepower [kW]"], float_format='%.1f')

            columncount = 4
            for carc in range(numberofcars):
                exc_ev_obtpower = pd.DataFrame(eV[carc].a_plugtime[count] for count in range(twodaycount))
                exc_ev_obtpower.to_excel(writer, sheet_name='Plug Time EV´s', index=False, startcol=columncount,
                                         header=["EV %d Plug time" % (carc + 1)], float_format='%.1f')
                columncount += 1

            writer.save()  # exports to Excel
            writer.close()

    # -------------Distribute Energy-------------
    def calc_fulfillment_fleet():
        # calculate Fulfillment of Energydemand in %
        sum_energydemand_all = 0
        remain_energydemand_all = 0
        sum_charged_energy = 0

        for ccount in range(numberofcars):
            sum_energydemand_all += eV[ccount].energydemand
            remain_energydemand_all += eV[ccount].final_energydemand
            # remaining energy to fill
        sum_charged_energy = sum_energydemand_all - remain_energydemand_all

        fulfillment = (1 - remain_energydemand_all / sum_energydemand_all) * 100
        #
        # if fulfillment > 99.5:
        #     for ccount in range(numberofcars):
        #         if eV[ccount].final_energydemand != 0:
        #             print(str(ccount) + "EV Remaining energy demand " + str(eV[ccount].final_energydemand)+ "EV Original energy demand " +  str(eV[ccount].energydemand))

        return fulfillment, sum_charged_energy

    def set_rem_energydemand(timing, num):
        if eV[num].a_plugtime[timing - 1] == 0 and eV[num].a_plugtime[timing] == 1:
            eV[num].remain_energydemand = eV[num].energydemand

    def set_final_energydemand(timing, num):
        if (eV[num].departure + onedaycount - 1) <= timing < (eV[num].departure + onedaycount):
            eV[num].final_energydemand = eV[num].remain_energydemand

    def beautify_rem_energydemand(num):
        if eV[num].remain_energydemand < 0.02:
            # solely for monitoring, you don´t want values lower than 0
            eV[num].remain_energydemand = 0

    def set_chargingstatus(timing, num):
        if eV[num].remain_energydemand > 0 and eV[num].a_plugtime[timing] == 1:
            eV[num].chargingstatus[timing] = 1

    def distribute_energy_car(timing, carc):
        if eV[carc].remain_energydemand > 0 and eV[carc].obt_chargepower[timing] > 0:
            eV[carc].remain_energydemand -= eV[carc].obt_chargepower[timing] * (1 / 4) * \
                                                calc_charging_efficiency(carc, timing)
            if eV[carc].remain_energydemand < 0.02:
                eV[carc].remain_energydemand = 0
                eV[carc].chargingstatus[timing] = 0

    def calc_min_power_depot_left(timing):
        min_power = 2000
        for ev in eV:
            if ev.chargingstatus[timing] == 1 and ev.min_chpower < min_power:
                min_power = ev.min_chpower
        return min_power

    def calc_charging_efficiency(carcount, timing):
        ch_eff = 0
        if eV[carcount].obt_chargepower[timing] < 0.33 * eV[carcount].chargepower:
            ch_eff = charging_efficiency[2]
        elif eV[carcount].obt_chargepower[timing] < 0.66 * eV[carcount].chargepower:
            ch_eff = charging_efficiency[1]
        else:
            ch_eff = charging_efficiency[0]
        eV[carcount].obt_ch_eff[timing] = ch_eff
        return ch_eff

    def needed_power(carc, timing):
        if eV[carc].chargepower * (1 / 4) * calc_charging_efficiency(carc, timing) > eV[carc].remain_energydemand:
            power = (eV[carc].remain_energydemand / (1 / 4 * calc_charging_efficiency(carc, timing)))
            if power < eV[carc].min_chpower:
                power = eV[carc].min_chpower
            return power
        else:
            return eV[carc].chargepower

    def distribute_little_power(timing):
        # is there power to distribute, thats bigger than min power of prios
        # distribute_min_power(timing, 0, prio_count)

                for carc in range(numberofcars):  # better: only go through prios
                    car_number = sortedlist[carc].number - 1  # get the order out of the sortedlist => number of car is in object

                    if eV[car_number].chargingstatus[timing] == 1 and distributable_kW[timing] >= eV[car_number].min_chpower:
                        if distributable_kW[timing] >= eV[car_number].chargepower:
                            chargepower = eV[car_number].chargepower
                        elif distributable_kW[timing] >= eV[car_number].min_chpower:
                            chargepower = distributable_kW[timing]
                        else:
                            chargepower = 0

                        wanted_power = needed_power(car_number, timing)
                        if chargepower >= wanted_power:
                            eV[car_number].obt_chargepower[timing] = wanted_power
                        else:
                            eV[car_number].obt_chargepower[timing] = chargepower

                        distributable_kW[timing] -= eV[car_number].obt_chargepower[timing]
                        distribute_energy_car(timing, car_number)

    def list_sort():
        if how_to_sort == 1:
            return sorted(eV, key=attrgetter('prioritized', 'must_charge',
                                         'last_timestep_charging', 'urg_coefficient', 'arrival'))
        elif how_to_sort == 2:
            return sorted(eV, key=attrgetter('prioritized', 'last_timestep_charging',
                                          'urg_coefficient', 'arrival'))
        elif how_to_sort == 3:
            return sorted(eV, key=attrgetter('must_charge', 'prioritized', 'departure', 'arrival'))
        elif how_to_sort == 4:
            return sorted(eV, key=attrgetter('prioritized', 'departure', 'arrival'))
        elif how_to_sort == 5:
            return sorted(eV, key=attrgetter('must_charge', 'prioritized', 'last_timestep_charging', 'departure', 'arrival'))

    kW_steps = 0.01
    actual_chargedpower_all = [0 for count in range(twodaycount)]

    for time in range(twodaycount):
        for ev in eV:
            set_rem_energydemand(time, ev.number-1)
            set_chargingstatus(time, ev.number-1)
            if ev.chargingstatus[time] == 1:
                a_chargepower_all[time] += ev.chargepower
            ev.urgency_coefficient(time)
            ev.last_time_charging(time)
            ev.is_must_charge(time)

        sortedlist = list_sort()

        if distributable_kW[time] >= calc_min_power_depot_left(time):
            distribute_little_power(time)

        for carc in range(numberofcars):
            actual_chargedpower_all[time] += eV[carc].obt_chargepower[time]
            beautify_rem_energydemand(carc)
            set_final_energydemand(time, carc)

    fulfillment_degree, charged_energy_all = calc_fulfillment_fleet()

    # input_df = pd.read_excel(read_file, sheet_name=0, index_col=0, usecols="A:B")
    # desired_fulfillment = input_df.loc['Desired Fulfillment'][0]

    # if (iterate_count != 0 or iterate_count == 'limit') and (fulfillment_degree >= desired_fulfillment or iterate_count == 'limit' or iterate_count == 'unmanaged'):
    if (iterate_count == 1 or iterate_count == 'unmanaged'):
        export_data_excel()

    # -------------------------------- End of Distribution Calc -----------------------------------------

    return [a_gridmax, fulfillment_degree,actual_chargedpower_all,a_siteload,eV,numberofcars]


if __name__ == '__main__':

    x = main(file,grid_max,iterate_count,Folder,season)


