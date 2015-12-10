#!/usr/bin/python

import zipfile
import sqlite3
import re
import sys
import datetime

#Data processing functions by ID that return a dictionary with the data ID and value
####################################################################################
def ControlBatteryCmds(data, id):                                                  #
    return {                                                                       #
        'FanSwitch': (data>>63)&0x1,                                               #
        'BattTracCnnct_D_Rq': (data>>43)&0x3,                                      #
        'CellBalSwitch_Expd': (data>>41)&0x3                                       #
    }                                                                              #
                                                                                   #
def HVCyclerStatus(data, id):                                                      #
    return {                                                                       #
        'HV_Cycler_on_status': (data>>63)&0x1,                                     #
        'HV_Cycler_on_cmd': (data>>62)&0x1,                                        #
        'HV_Cycler_Power_cmd': ((data>>48)&0x7FF)*100-102400,                      #
        'HV_Cycler_Current_Actl': ((data>>32)&0x7FFF)*0.05-750,                    #
        'HV_Cycler_Voltage_Actl': ((data>>8)&0xFFF)*0.1                            #
    }                                                                              #
                                                                                   #
def Battery_Traction_1(data, id):                                                  #
    return {                                                                       #
        'BattTracCnnct_B_Cmd': (data>>63)&0x1,                                     #
        'BattTrac_I_Actl': ((data>>48)&0x7FFF)*0.05-750,                           #
        'BattTracOff_B_Actl': (data>>44)&0x1,                                      #
        'BattTracMil_D_Rq': (data>>42)&0x3,                                        #
        'BattTrac_U_Actl': ((data>>32)&0x3FF)*0.5,                                 #
        'BattTrac_U_LimHi': ((data>>24)&0xFF)*2,                                   #
        'BattTrac_U_LimLo': ((data>>16)&0xFF)*2                                    #
    }                                                                              #
                                                                                   #
def LVMVCyclerStatus(data, id):                                                    #
    return {                                                                       #
        'LV_Cycler_cmd': (data>>62)&0x3,                                           #
        'LV_Cycler_Current': ((data>>48)&0x7FF)*0.05-20,                           #
        'LV_Cycler_Voltage': ((data>>40)&0xFF)*0.1,                                #
        'MV_Cycler_cmd': (data>>30)&0x3,                                           #
        'MV_Cycler_status': (data>>28)&0x3,                                        #
        'MV_Cycler_Current': ((data>>16)&0x7FF)*0.05-20,                           #
        'MV_Cycler_Voltage': (data>>8)&0xFF                                        #
    }                                                                              #
                                                                                   #
def ControlConverterCmds(data, id):                                                #
    return {                                                                       #
        'ConverterEnable': (data>>63)&0x1,                                         #
        'ConverterCalibrate': (data>>61)&0x3,                                      #
        'CalibrationVoltage': ((data>>40)&0x1FFF)*0.0001+10,                       #
        'Objective_Map_Rq': (data>>37)&0x7,                                        #
        'Objective_Map_LifeGain': ((data>>24)&0xFF)*0.0001                         #
    }                                                                              #
                                                                                   #
def ManualConverterCmds(data, id):                                                 #
    return {                                                                       #
        'ConverterEnableSampling': (data>>62)&0x3,                                 #
        'ConverterEnableSwitching': (data>>60)&0x3,                                #
        'ConverterOpenClosedLoop': (data>>58)&0x3,                                 #
        'Phase_rq': (data>>32)&0xFFFFFF,                                           #
        'DestinationAddress': (data>>24)&0xFF,                                     #
        'DestinationAddressCap': (data>>16)&0xFF,                                  #
        'CellCapacity': (data&0x7FF)*0.01+7                                        #
    }                                                                              #
                                                                                   #
def SetConverterCellLimits(data, id):                                              #
    return {                                                                       #
        'HighCellVoltageLimit': ((data>>48)&0xFFFF)*0.0001,                        #
        'LowCellVoltageLimit': ((data>>32)&0xFFFF)*0.0001,                         #
        'HighCellCurrentLimit': ((data>>16)&0xFFFF)*0.001-32.765,                  #
        'LowCellCurrentLimit': ((data)&0xFFFF)*0.001-32.765                        #
    }                                                                              #
                                                                                   #
def SetConverterBusLimits(data, id):                                               #
    return {                                                                       #
        'HighBusVoltageLimit': ((data>>48)&0xFFFF)*0.001,                          #
        'LowBusVoltageLimit': ((data>>32)&0xFFFF)*0.001                            #
    }                                                                              #
                                                                                   #
def TargetStatus(data, id):                                                        #
    return {                                                                       #
        'System_Mode': (data>>62)&0x3,                                             #
        'Operating_Mode': (data>>59)&0x7,                                          #
        'Objective_Map_Actl': (data>>56)&0x7,                                      #
        'Status_Normal': (data>>55)&0x1,                                           #
        'Status_System_Fault': (data>>54)&0x1,                                     #
        'Status_Saturation': (data>>53)&0x1,                                       #
        'Status_Comm_Fault': (data>>52)&0x1,                                       #
        'Status_Converter_Fault': (data>>51)&0x1,                                  #
        'Comm_Uptime_Percent': ((data>>40)&0x3FF)*0.1                              #
    }                                                                              #
                                                                                   #
def Battery_Traction_2(data, id):                                                  #
    return {                                                                       #
        'BattTrac_Min_CellVolt': ((data>>48)&0xFFFF)*0.0001,                       #
        'BattTrac_Max_CellVolt': ((data>>32)&0xFFFF)*0.0001,                       #
        'BattTrac_Pw_LimChrg': ((data>>16)&0x3FF)*250,                             #
        'BattTrac_Pw_LimDchrg': ((data)&0x3FF)*250                                 #
    }                                                                              #
                                                                                   #
def Battery_Traction_3(data, id):                                                  #
    return {                                                                       #
        'BattTracWarnLamp_B_Rq': (data>>59)&0x1,                                   #
        'BattTracSrvcRqd_B_Rq': (data>>58)&0x1,                                    #
        'BattTrac_Min_CellTemp': ((data>>48)&0x3FF)*0.5-50,                        #
        'BattTrac_Max_CellTemp': ((data>>32)&0x3FF)*0.5-50,                        #
        'BattTracSoc_Pc_MnPrtct': ((data>>16)&0x3FF)*0.1,                          #
        'BattTracSoc_Pc_MxPrtct': ((data)&0x3FF)*0.1                               #
    }                                                                              #
                                                                                   #
def Battery_Traction_4(data, id):                                                  #
    return {                                                                       #
        'BattTracClntIn_Te_Actl': ((data>>56)&0xFF)-50,                            #
        'BattTracCool_D_Falt': (data>>50)&0x3,                                     #
        'BattTrac_Te_Actl': ((data>>40)&0x3FF)*0.5-50,                             #
        'BattTracSoc2_Pc_Actl': ((data>>24)&0x3FFF)*0.01,                          #
        'HvacAir_Flw_EstBatt': ((data>>16)&0xFF)*0.5,                              #
        'BattTracSoc_Pc_Dsply': ((data>>8)&0xFF)*0.5                               #
    }                                                                              #
                                                                                   #
def Battery_Traction_5(data, id):                                                  #
    return {                                                                       #
        'BattTracSoc_Min_UHP': ((data>>48)&0x3FF)*0.1,                             #
        'BattTracSoc_Max_UHP': ((data>>32)&0x3FF)*0.1,                             #
        'BattTracSoc_Min_LHP': ((data>>16)&0x3FF)*0.1,                             #
        'BattTracSoc_Max_LHP': ((data)&0x3FF)*0.1                                  #
    }                                                                              #
                                                                                   #
def Unknown(data, id):                                                             #
    return {                                                                       #
        id: data                                                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_1(data, id):                                                  #
    return {                                                                       #
        'CellVoltage_1': ((data>>48)&0xFFFF)*0.0001,                               #
        'CellVoltage_2': ((data>>32)&0xFFFF)*0.0001,                               #
        'CellVoltage_3': ((data>>16)&0xFFFF)*0.0001,                               #
        'CellVoltage_4': ((data)&0xFFFF)*0.0001                                    #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_2(data, id):                                                  #
    return {                                                                       #
        'CellVoltage_5': ((data>>48)&0xFFFF)*0.0001,                               #
        'CellVoltage_6': ((data>>32)&0xFFFF)*0.0001,                               #
        'CellVoltage_7': ((data>>16)&0xFFFF)*0.0001,                               #
        'CellVoltage_8': ((data)&0xFFFF)*0.0001                                    #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_3(data, id):                                                  #
    return {                                                                       #
        'CellVoltage_9': ((data>>48)&0xFFFF)*0.0001,                               #
        'CellVoltage_10': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_11': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_12': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_4(data, id):                                                  #
    return {                                                                       #
        'CellVoltage_13': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_14': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_15': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_16': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_5(data, id):                                                  #
    return {                                                                       #
        'CellVoltage_17': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_18': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_19': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_20': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_6(data, id):                                                  #
    return {                                                                       #
        'CellVoltage_21': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_22': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_23': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_24': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_7(data, id):                                                  #
    return {                                                                       #
        'CellVoltage_25': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_26': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_27': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_28': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_8(data, id):                                                  #
    return {                                                                       #
        'CellVoltage_29': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_30': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_31': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_32': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_9(data, id):                                                  #
    return {                                                                       #
        'CellVoltage_33': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_34': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_35': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_36': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_10(data, id):                                                 #
    return {                                                                       #
        'CellVoltage_37': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_38': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_39': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_40': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_11(data, id):                                                 #
    return {                                                                       #
        'CellVoltage_41': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_42': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_43': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_44': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_12(data, id):                                                 #
    return {                                                                       #
        'CellVoltage_45': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_46': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_47': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_48': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_13(data, id):                                                 #
    return {                                                                       #
        'CellVoltage_49': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_50': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_51': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_52': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_14(data, id):                                                 #
    return {                                                                       #
        'CellVoltage_53': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_54': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_55': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_56': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_15(data, id):                                                 #
    return {                                                                       #
        'CellVoltage_57': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_58': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_59': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_60': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_16(data, id):                                                 #
    return {                                                                       #
        'CellVoltage_61': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_62': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_63': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_64': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_17(data, id):                                                 #
    return {                                                                       #
        'CellVoltage_65': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_66': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_67': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_68': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_18(data, id):                                                 #
    return {                                                                       #
        'CellVoltage_69': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_70': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_71': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_72': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_19(data, id):                                                 #
    return {                                                                       #
        'CellVoltage_73': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_74': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_75': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_76': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_20(data, id):                                                 #
    return {                                                                       #
        'CellVoltage_77': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_78': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_79': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_80': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellVoltageGroup_21(data, id):                                                 #
    return {                                                                       #
        'CellVoltage_81': ((data>>48)&0xFFFF)*0.0001,                              #
        'CellVoltage_82': ((data>>32)&0xFFFF)*0.0001,                              #
        'CellVoltage_83': ((data>>16)&0xFFFF)*0.0001,                              #
        'CellVoltage_84': ((data)&0xFFFF)*0.0001                                   #
    }                                                                              #
                                                                                   #
def CellCurrentGroup_1(data, id):                                                  #
    return {                                                                       #
        'CellCurrent_1': ((data>>48)&0xFFFF)*0.001-32.765,                         #
        'CellCurrent_2': ((data>>32)&0xFFFF)*0.001-32.765,                         #
        'CellCurrent_3': ((data>>16)&0xFFFF)*0.001-32.765,                         #
        'CellCurrent_4': ((data)&0xFFFF)*0.001-32.765                              #
    }                                                                              #
                                                                                   #
def CellCurrentGroup_2(data, id):                                                  #
    return {                                                                       #
        'CellCurrent_5': ((data>>48)&0xFFFF)*0.001-32.765,                         #
        'CellCurrent_6': ((data>>32)&0xFFFF)*0.001-32.765,                         #
        'CellCurrent_7': ((data>>16)&0xFFFF)*0.001-32.765,                         #
        'CellCurrent_8': ((data)&0xFFFF)*0.001-32.765                              #
    }                                                                              #
                                                                                   #
def CellCurrentGroup_3(data, id):                                                  #
    return {                                                                       #
        'CellCurrent_9': ((data>>48)&0xFFFF)*0.001-32.765,                         #
        'CellCurrent_10': ((data>>32)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_11': ((data>>16)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_12': ((data)&0xFFFF)*0.001-32.765                             #
    }                                                                              #
                                                                                   #
def CellCurrentGroup_4(data, id):                                                  #
    return {                                                                       #
        'CellCurrent_13': ((data>>48)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_14': ((data>>32)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_15': ((data>>16)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_16': ((data)&0xFFFF)*0.001-32.765                             #
    }                                                                              #
                                                                                   #
def CellCurrentGroup_5(data, id):                                                  #
    return {                                                                       #
        'CellCurrent_17': ((data>>48)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_18': ((data>>32)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_19': ((data>>16)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_20': ((data)&0xFFFF)*0.001-32.765                             #
    }                                                                              #
                                                                                   #
def CellCurrentGroup_6(data, id):                                                  #
    return {                                                                       #
        'CellCurrent_21': ((data>>48)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_22': ((data>>32)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_23': ((data>>16)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_24': ((data)&0xFFFF)*0.001-32.765                             #
    }                                                                              #
                                                                                   #
def CellCurrentGroup_7(data, id):                                                  #
    return {                                                                       #
        'CellCurrent_25': ((data>>48)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_26': ((data>>32)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_27': ((data>>16)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_28': ((data)&0xFFFF)*0.001-32.765                             #
    }                                                                              #
                                                                                   #
def CellCurrentGroup_8(data, id):                                                  #
    return {                                                                       #
        'CellCurrent_29': ((data>>48)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_30': ((data>>32)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_31': ((data>>16)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_32': ((data)&0xFFFF)*0.001-32.765                             #
    }                                                                              #
                                                                                   #
def CellCurrentGroup_9(data, id):                                                  #
    return {                                                                       #
        'CellCurrent_33': ((data>>48)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_34': ((data>>32)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_35': ((data>>16)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_36': ((data)&0xFFFF)*0.001-32.765                             #
    }                                                                              #
                                                                                   #
def CellCurrentGroup_10(data, id):                                                 #
    return {                                                                       #
        'CellCurrent_37': ((data>>48)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_38': ((data>>32)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_39': ((data>>16)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_40': ((data)&0xFFFF)*0.001-32.765                             #
    }                                                                              #
                                                                                   #
def CellCurrentGroup_11(data, id):                                                 #
    return {                                                                       #
        'CellCurrent_41': ((data>>48)&0xFFFF)*0.001-32.765,                        #
        'CellCurrent_42': ((data>>32)&0xFFFF)*0.001-32.765                         #
    }                                                                              #
                                                                                   #
                                                                                   #
def BECMCellTempGroup_1(data, id):                                                 #
    return {                                                                       #
        'BECM_CellTemp_1': ((data>>48)&0x7FF)*0.1-40,                              #
        'BECM_CellTemp_2': ((data>>32)&0x7FF)*0.1-40,                              #
        'BECM_CellTemp_3': ((data>>16)&0x7FF)*0.1-40,                              #
        'BECM_CellTemp_4': ((data)&0x7FF)*0.1-40                                   #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_2(data, id):                                                 #
    return {                                                                       #
        'BECM_CellTemp_5': ((data>>48)&0x7FF)*0.1-40,                              #
        'BECM_CellTemp_6': ((data>>32)&0x7FF)*0.1-40,                              #
        'BECM_CellTemp_7': ((data>>16)&0x7FF)*0.1-40,                              #
        'BECM_CellTemp_8': ((data)&0x7FF)*0.1-40                                   #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_3(data, id):                                                 #
    return {                                                                       #
        'BECM_CellTemp_10': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_11': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_12': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_9': ((data)&0x7FF)*0.1-40                                   #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_4(data, id):                                                 #
    return {                                                                       #
        'BECM_CellTemp_13': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_14': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_15': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_16': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_5(data, id):                                                 #
    return {                                                                       #
        'BECM_CellTemp_17': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_18': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_19': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_20': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_6(data, id):                                                 #
    return {                                                                       #
        'BECM_CellTemp_21': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_22': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_23': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_24': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_7(data, id):                                                 #
    return {                                                                       #
        'BECM_CellTemp_25': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_26': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_27': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_28': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_8(data, id):                                                 #
    return {                                                                       #
        'BECM_CellTemp_29': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_30': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_31': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_32': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_9(data, id):                                                 #
    return {                                                                       #
        'BECM_CellTemp_33': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_34': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_35': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_36': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_10(data, id):                                                #
    return {                                                                       #
        'BECM_CellTemp_37': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_38': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_39': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_40': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_11(data, id):                                                #
    return {                                                                       #
        'BECM_CellTemp_41': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_42': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_43': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_44': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_12(data, id):                                                #
    return {                                                                       #
        'BECM_CellTemp_45': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_46': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_47': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_48': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_13(data, id):                                                #
    return {                                                                       #
        'BECM_CellTemp_49': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_50': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_51': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_52': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_14(data, id):                                                #
    return {                                                                       #
        'BECM_CellTemp_53': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_54': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_55': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_56': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_15(data, id):                                                #
    return {                                                                       #
        'BECM_CellTemp_57': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_58': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_59': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_60': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_16(data, id):                                                #
    return {                                                                       #
        'BECM_CellTemp_61': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_62': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_63': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_64': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_17(data, id):                                                #
    return {                                                                       #
        'BECM_CellTemp_65': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_66': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_67': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_68': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_18(data, id):                                                #
    return {                                                                       #
        'BECM_CellTemp_69': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_70': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_71': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_72': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_19(data, id):                                                #
    return {                                                                       #
        'BECM_CellTemp_73': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_74': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_75': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_76': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_20(data, id):                                                #
    return {                                                                       #
        'BECM_CellTemp_77': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_78': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_79': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_80': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def BECMCellTempGroup_21(data, id):                                                #
    return {                                                                       #
        'BECM_CellTemp_81': ((data>>48)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_82': ((data>>32)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_83': ((data>>16)&0x7FF)*0.1-40,                             #
        'BECM_CellTemp_84': ((data)&0x7FF)*0.1-40                                  #
    }                                                                              #
                                                                                   #
def CellTempGroup_1(data, id):                                                     #
    return {                                                                       #
        'CellTemp_1': ((data>>48)&0x7FF)*0.1-40,                                   #
        'CellTemp_2': ((data>>32)&0x7FF)*0.1-40,                                   #
        'CellTemp_3': ((data>>16)&0x7FF)*0.1-40,                                   #
        'CellTemp_4': ((data)&0x7FF)*0.1-40                                        #
    }                                                                              #
                                                                                   #
def CellTempGroup_2(data, id):                                                     #
    return {                                                                       #
        'CellTemp_5': ((data>>48)&0x7FF)*0.1-40,                                   #
        'CellTemp_6': ((data>>32)&0x7FF)*0.1-40,                                   #
        'CellTemp_7': ((data>>16)&0x7FF)*0.1-40,                                   #
        'CellTemp_8': ((data)&0x7FF)*0.1-40                                        #
    }                                                                              #
                                                                                   #
def CellTempGroup_3(data, id):                                                     #
    return {                                                                       #
        'CellTemp_9': ((data>>48)&0x7FF)*0.1-40,                                   #
        'CellTemp_10': ((data>>32)&0x7FF)*0.1-40,                                  #
        'CellTemp_11': ((data>>16)&0x7FF)*0.1-40,                                  #
        'CellTemp_12': ((data)&0x7FF)*0.1-40                                       #
    }                                                                              #
                                                                                   #
def CellTempGroup_4(data, id):                                                     #
    return {                                                                       #
        'CellTemp_13': ((data>>48)&0x7FF)*0.1-40,                                  #
        'CellTemp_14': ((data>>32)&0x7FF)*0.1-40,                                  #
        'CellTemp_15': ((data>>16)&0x7FF)*0.1-40,                                  #
        'CellTemp_16': ((data)&0x7FF)*0.1-40                                       #
    }                                                                              #
                                                                                   #
def CellTempGroup_5(data, id):                                                     #
    return {                                                                       #
        'CellTemp_17': ((data>>48)&0x7FF)*0.1-40,                                  #
        'CellTemp_18': ((data>>32)&0x7FF)*0.1-40,                                  #
        'CellTemp_19': ((data>>16)&0x7FF)*0.1-40,                                  #
        'CellTemp_20': ((data)&0x7FF)*0.1-40                                       #
    }                                                                              #
                                                                                   #
def CellTempGroup_6(data, id):                                                     #
    return {                                                                       #
        'CellTemp_21': ((data>>48)&0x7FF)*0.1-40,                                  #
        'CellTemp_22': ((data>>32)&0x7FF)*0.1-40,                                  #
        'CellTemp_23': ((data>>16)&0x7FF)*0.1-40,                                  #
        'CellTemp_24': ((data)&0x7FF)*0.1-40                                       #
    }                                                                              #
                                                                                   #
def CellTempGroup_7(data, id):                                                     #
    return {                                                                       #
        'CellTemp_25': ((data>>48)&0x7FF)*0.1-40,                                  #
        'CellTemp_26': ((data>>32)&0x7FF)*0.1-40,                                  #
        'CellTemp_27': ((data>>16)&0x7FF)*0.1-40,                                  #
        'CellTemp_28': ((data)&0x7FF)*0.1-40                                       #
    }                                                                              #
                                                                                   #
def CellTempGroup_8(data, id):                                                     #
    return {                                                                       #
        'CellTemp_29': ((data>>48)&0x7FF)*0.1-40,                                  #
        'CellTemp_30': ((data>>32)&0x7FF)*0.1-40,                                  #
        'CellTemp_31': ((data>>16)&0x7FF)*0.1-40,                                  #
        'CellTemp_32': ((data)&0x7FF)*0.1-40                                       #
    }                                                                              #
                                                                                   #
def CellTempGroup_9(data, id):                                                     #
    return {                                                                       #
        'CellTemp_33': ((data>>48)&0x7FF)*0.1-40,                                  #
        'CellTemp_34': ((data>>32)&0x7FF)*0.1-40,                                  #
        'CellTemp_35': ((data>>16)&0x7FF)*0.1-40,                                  #
        'CellTemp_36': ((data)&0x7FF)*0.1-40                                       #
    }                                                                              #
                                                                                   #
def CellTempGroup_10(data, id):                                                    #
    return {                                                                       #
        'CellTemp_37': ((data>>48)&0x7FF)*0.1-40,                                  #
        'CellTemp_38': ((data>>32)&0x7FF)*0.1-40,                                  #
        'CellTemp_39': ((data>>16)&0x7FF)*0.1-40,                                  #
        'CellTemp_40': ((data)&0x7FF)*0.1-40                                       #
    }                                                                              #
                                                                                   #
def CellTempGroup_11(data, id):                                                    #
    return {                                                                       #
        'CellTemp_41': ((data>>48)&0x7FF)*0.1-40,                                  #
        'CellTemp_42': ((data>>32)&0x7FF)*0.1-40,                                  #
        'CellTemp_43': ((data>>16)&0x7FF)*0.1-40,                                  #
        'CellTemp_44': ((data)&0x7FF)*0.1-40                                       #
    }                                                                              #
                                                                                   #
def CellTempGroup_12(data, id):                                                    #
    return {                                                                       #
        'CellTemp_45': ((data>>48)&0x7FF)*0.1-40,                                  #
        'CellTemp_46': ((data>>32)&0x7FF)*0.1-40,                                  #
        'CellTemp_47': ((data>>16)&0x7FF)*0.1-40,                                  #
        'CellTemp_48': ((data)&0x7FF)*0.1-40                                       #
    }                                                                              #
                                                                                   #
def CellTempGroup_13(data, id):                                                    #
    return {                                                                       #
        'CellTemp_49': ((data>>48)&0x7FF)*0.1-40,                                  #
        'CellTemp_50': ((data>>32)&0x7FF)*0.1-40,                                  #
        'CellTemp_51': ((data>>16)&0x7FF)*0.1-40,                                  #
        'CellTemp_52': ((data)&0x7FF)*0.1-40                                       #
    }                                                                              #
                                                                                   #
def CellTempGroup_14(data, id):                                                    #
    return {                                                                       #
        'CellTemp_53': ((data>>48)&0x7FF)*0.1-40,                                  #
        'CellTemp_54': ((data>>32)&0x7FF)*0.1-40,                                  #
        'CellTemp_55': ((data>>16)&0x7FF)*0.1-40,                                  #
        'CellTemp_56': ((data)&0x7FF)*0.1-40                                       #
    }                                                                              #
                                                                                   #
def CellTempGroup_15(data, id):                                                    #
    return {                                                                       #
        'BoardTemp_1': ((data>>48)&0x7FF)*0.1-40,                                  #
        'BoardTemp_2': ((data>>32)&0x7FF)*0.1-40,                                  #
        'BoardTemp_3': ((data>>16)&0x7FF)*0.1-40,                                  #
        'BoardTemp_4': ((data)&0x7FF)*0.1-40                                       #
    }                                                                              #
                                                                                   #
def CellTempGroup_16(data, id):                                                    #
    return {                                                                       #
        'BoardTemp_5': ((data>>48)&0x7FF)*0.1-40,                                  #
        'BoardTemp_6': ((data>>32)&0x7FF)*0.1-40,                                  #
        'BoardTemp_7': ((data>>16)&0x7FF)*0.1-40,                                  #
        'BoardTemp_8': ((data)&0x7FF)*0.1-40                                       #
    }                                                                              #
                                                                                   #
def CellTempGroup_17(data, id):                                                    #
    return {                                                                       #
        'BoardTemp_9': ((data>>48)&0x7FF)*0.1-40,                                  #
        'BoardTemp_10': ((data>>32)&0x7FF)*0.1-40,                                 #
        'BoardTemp_11': ((data>>16)&0x7FF)*0.1-40,                                 #
        'BoardTemp_12': ((data)&0x7FF)*0.1-40                                      #
    }                                                                              #
                                                                                   #
def CellTempGroup_18(data, id):                                                    #
    return {                                                                       #
        'BoardTemp_13': ((data>>48)&0x7FF)*0.1-40,                                 #
        'BoardTemp_14': ((data>>32)&0x7FF)*0.1-40,                                 #
        'BoardTemp_15': ((data>>16)&0x7FF)*0.1-40,                                 #
        'BoardTemp_16': ((data)&0x7FF)*0.1-40                                      #
    }                                                                              #
                                                                                   #
def CellTempGroup_19(data, id):                                                    #
    return {                                                                       #
        'BoardTemp_17': ((data>>48)&0x7FF)*0.1-40,                                 #
        'BoardTemp_18': ((data>>32)&0x7FF)*0.1-40,                                 #
        'BoardTemp_19': ((data>>16)&0x7FF)*0.1-40,                                 #
        'BoardTemp_20': ((data)&0x7FF)*0.1-40                                      #
    }                                                                              #
                                                                                   #
def CellTempGroup_20(data, id):                                                    #
    return {                                                                       #
        'BoardTemp_21': ((data>>48)&0x7FF)*0.1-40,                                 #
        'BoardTemp_22': ((data>>32)&0x7FF)*0.1-40,                                 #
        'BoardTemp_23': ((data>>16)&0x7FF)*0.1-40,                                 #
        'BoardTemp_24': ((data)&0x7FF)*0.1-40                                      #
    }                                                                              #
                                                                                   #
def CellTempGroup_21(data, id):                                                    #
    return {                                                                       #
        'BoardTemp_25': ((data>>48)&0x7FF)*0.1-40,                                 #
        'BoardTemp_26': ((data>>32)&0x7FF)*0.1-40,                                 #
        'BoardTemp_27': ((data>>16)&0x7FF)*0.1-40,                                 #
        'BoardTemp_28': ((data)&0x7FF)*0.1-40                                      #
    }                                                                              #
                                                                                   #
def USUCellVoltageGroup_1(data, id):                                               #
    return {                                                                       #
        'USUCellVoltage_1': ((data>>48)&0xFFFF)*0.0001,                            #
        'USUCellVoltage_2': ((data>>32)&0xFFFF)*0.0001,                            #
        'USUCellVoltage_3': ((data>>16)&0xFFFF)*0.0001,                            #
        'USUCellVoltage_4': ((data)&0xFFFF)*0.0001                                 #
    }                                                                              #
                                                                                   #
def USUCellVoltageGroup_2(data, id):                                               #
    return {                                                                       #
        'USUCellVoltage_5': ((data>>48)&0xFFFF)*0.0001,                            #
        'USUCellVoltage_6': ((data>>32)&0xFFFF)*0.0001,                            #
        'USUCellVoltage_7': ((data>>16)&0xFFFF)*0.0001,                            #
        'USUCellVoltage_8': ((data)&0xFFFF)*0.0001                                 #
    }                                                                              #
                                                                                   #
def USUCellVoltageGroup_3(data, id):                                               #
    return {                                                                       #
        'USUCellVoltage_9': ((data>>48)&0xFFFF)*0.0001,                            #
        'USUCellVoltage_10': ((data>>32)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_11': ((data>>16)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_12': ((data)&0xFFFF)*0.0001                                #
    }                                                                              #
                                                                                   #
def USUCellVoltageGroup_4(data, id):                                               #
    return {                                                                       #
        'USUCellVoltage_13': ((data>>48)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_14': ((data>>32)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_15': ((data>>16)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_16': ((data)&0xFFFF)*0.0001                                #
    }                                                                              #
                                                                                   #
def USUCellVoltageGroup_5(data, id):                                               #
    return {                                                                       #
        'USUCellVoltage_17': ((data>>48)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_18': ((data>>32)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_19': ((data>>16)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_20': ((data)&0xFFFF)*0.0001                                #
    }                                                                              #
                                                                                   #
def USUCellVoltageGroup_6(data, id):                                               #
    return {                                                                       #
        'USUCellVoltage_21': ((data>>48)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_22': ((data>>32)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_23': ((data>>16)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_24': ((data)&0xFFFF)*0.0001                                #
    }                                                                              #
                                                                                   #
def USUCellVoltageGroup_7(data, id):                                               #
    return {                                                                       #
        'USUCellVoltage_25': ((data>>48)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_26': ((data>>32)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_27': ((data>>16)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_28': ((data)&0xFFFF)*0.0001                                #
    }                                                                              #
                                                                                   #
def USUCellVoltageGroup_8(data, id):                                               #
    return {                                                                       #
        'USUCellVoltage_29': ((data>>48)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_30': ((data>>32)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_31': ((data>>16)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_32': ((data)&0xFFFF)*0.0001                                #
    }                                                                              #
                                                                                   #
def USUCellVoltageGroup_9(data, id):                                               #
    return {                                                                       #
        'USUCellVoltage_33': ((data>>48)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_34': ((data>>32)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_35': ((data>>16)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_36': ((data)&0xFFFF)*0.0001                                #
    }                                                                              #
                                                                                   #
def USUCellVoltageGroup_10(data, id):                                              #
    return {                                                                       #
        'USUCellVoltage_37': ((data>>48)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_38': ((data>>32)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_39': ((data>>16)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_40': ((data)&0xFFFF)*0.0001                                #
    }                                                                              #
                                                                                   #
def USUCellVoltageGroup_11(data, id):                                              #
    return {                                                                       #
        'USUCellVoltage_41': ((data>>48)&0xFFFF)*0.0001,                           #
        'USUCellVoltage_42': ((data>>32)&0xFFFF)*0.0001                            #
    }                                                                              #
                                                                                   #
def USUCellSOCGroup_1(data, id):                                                   #
    return {                                                                       #
        'USUCellSOC_1': ((data>>48)&0x3FFF)*0.01,                                  #
        'USUCellSOC_2': ((data>>32)&0x3FFF)*0.01,                                  #
        'USUCellSOC_3': ((data>>16)&0x3FFF)*0.01,                                  #
        'USUCellSOC_4': ((data)&0x3FFF)*0.01                                       #
    }                                                                              #
                                                                                   #
def USUCellSOCGroup_2(data, id):                                                   #
    return {                                                                       #
        'USUCellSOC_5': ((data>>48)&0x3FFF)*0.01,                                  #
        'USUCellSOC_6': ((data>>32)&0x3FFF)*0.01,                                  #
        'USUCellSOC_7': ((data>>16)&0x3FFF)*0.01,                                  #
        'USUCellSOC_8': ((data)&0x3FFF)*0.01                                       #
    }                                                                              #
                                                                                   #
def USUCellSOCGroup_3(data, id):                                                   #
    return {                                                                       #
        'USUCellSOC_9': ((data>>48)&0x3FFF)*0.01,                                  #
        'USUCellSOC_10': ((data>>32)&0x3FFF)*0.01,                                 #
        'USUCellSOC_11': ((data>>16)&0x3FFF)*0.01,                                 #
        'USUCellSOC_12': ((data)&0x3FFF)*0.01                                      #
    }                                                                              #
                                                                                   #
def USUCellSOCGroup_4(data, id):                                                   #
    return {                                                                       #
        'USUCellSOC_13': ((data>>48)&0x3FFF)*0.01,                                 #
        'USUCellSOC_14': ((data>>32)&0x3FFF)*0.01,                                 #
        'USUCellSOC_15': ((data>>16)&0x3FFF)*0.01,                                 #
        'USUCellSOC_16': ((data)&0x3FFF)*0.01                                      #
    }                                                                              #
                                                                                   #
def USUCellSOCGroup_5(data, id):                                                   #
    return {                                                                       #
        'USUCellSOC_17': ((data>>48)&0x3FFF)*0.01,                                 #
        'USUCellSOC_18': ((data>>32)&0x3FFF)*0.01,                                 #
        'USUCellSOC_19': ((data>>16)&0x3FFF)*0.01,                                 #
        'USUCellSOC_20': ((data)&0x3FFF)*0.01                                      #
    }                                                                              #
                                                                                   #
def USUCellSOCGroup_6(data, id):                                                   #
    return {                                                                       #
        'USUCellSOC_21': ((data>>48)&0x3FFF)*0.01,                                 #
        'USUCellSOC_22': ((data>>32)&0x3FFF)*0.01,                                 #
        'USUCellSOC_23': ((data>>16)&0x3FFF)*0.01,                                 #
        'USUCellSOC_24': ((data)&0x3FFF)*0.01                                      #
    }                                                                              #
                                                                                   #
def USUCellSOCGroup_7(data, id):                                                   #
    return {                                                                       #
        'USUCellSOC_25': ((data>>48)&0x3FFF)*0.01,                                 #
        'USUCellSOC_26': ((data>>32)&0x3FFF)*0.01,                                 #
        'USUCellSOC_27': ((data>>16)&0x3FFF)*0.01,                                 #
        'USUCellSOC_28': ((data)&0x3FFF)*0.01                                      #
    }                                                                              #
                                                                                   #
def USUCellSOCGroup_8(data, id):                                                   #
    return {                                                                       #
        'USUCellSOC_29': ((data>>48)&0x3FFF)*0.01,                                 #
        'USUCellSOC_30': ((data>>32)&0x3FFF)*0.01,                                 #
        'USUCellSOC_31': ((data>>16)&0x3FFF)*0.01,                                 #
        'USUCellSOC_32': ((data)&0x3FFF)*0.01                                      #
    }                                                                              #
                                                                                   #
def USUCellSOCGroup_9(data, id):                                                   #
    return {                                                                       #
        'USUCellSOC_33': ((data>>48)&0x3FFF)*0.01,                                 #
        'USUCellSOC_34': ((data>>32)&0x3FFF)*0.01,                                 #
        'USUCellSOC_35': ((data>>16)&0x3FFF)*0.01,                                 #
        'USUCellSOC_36': ((data)&0x3FFF)*0.01                                      #
    }                                                                              #
                                                                                   #
def USUCellSOCGroup_10(data, id):                                                  #
    return {                                                                       #
        'USUCellSOC_37': ((data>>48)&0x3FFF)*0.01,                                 #
        'USUCellSOC_38': ((data>>32)&0x3FFF)*0.01,                                 #
        'USUCellSOC_39': ((data>>16)&0x3FFF)*0.01,                                 #
        'USUCellSOC_40': ((data)&0x3FFF)*0.01                                      #
    }                                                                              #
                                                                                   #
def USUCellSOCGroup_11(data, id):                                                  #
    return {                                                                       #
        'USUCellSOC_41': ((data>>48)&0x3FFF)*0.01,                                 #
        'USUCellSOC_42': ((data>>32)&0x3FFF)*0.01,                                 #
        'USUSOCBounds_41': ((data>>24)&0xFF)*0.1,                                  #
        'USUSOCBounds_42': ((data>>16)&0xFF)*0.1,                                  #
        'USUBoardTemp_41': ((data>>8)&0xFF)-40,                                    #
        'USUBoardTemp_42': ((data)&0xFF)-40                                        #
    }                                                                              #
                                                                                   #
def USUSOCBoundsGroup_1(data, id):                                                 #
    return {                                                                       #
        'USUSOCBounds_1': ((data>>56)&0xFF)*0.1,                                   #
        'USUSOCBounds_2': ((data>>48)&0xFF)*0.1,                                   #
        'USUSOCBounds_3': ((data>>40)&0xFF)*0.1,                                   #
        'USUSOCBounds_4': ((data>>32)&0xFF)*0.1,                                   #
        'USUSOCBounds_5': ((data>>24)&0xFF)*0.1,                                   #
        'USUSOCBounds_6': ((data>>16)&0xFF)*0.1,                                   #
        'USUSOCBounds_7': ((data>>8)&0xFF)*0.1,                                    #
        'USUSOCBounds_8': ((data)&0xFF)*0.1                                        #
    }                                                                              #
                                                                                   #
def USUSOCBoundsGroup_2(data, id):                                                 #
    return {                                                                       #
        'USUSOCBounds_9': ((data>>56)&0xFF)*0.1,                                   #
        'USUSOCBounds_10': ((data>>48)&0xFF)*0.1,                                  #
        'USUSOCBounds_11': ((data>>40)&0xFF)*0.1,                                  #
        'USUSOCBounds_12': ((data>>32)&0xFF)*0.1,                                  #
        'USUSOCBounds_13': ((data>>24)&0xFF)*0.1,                                  #
        'USUSOCBounds_14': ((data>>16)&0xFF)*0.1,                                  #
        'USUSOCBounds_15': ((data>>8)&0xFF)*0.1,                                   #
        'USUSOCBounds_16': ((data)&0xFF)*0.1                                       #
    }                                                                              #
                                                                                   #
def USUSOCBoundsGroup_3(data, id):                                                 #
    return {                                                                       #
        'USUSOCBounds_17': ((data>>56)&0xFF)*0.1,                                  #
        'USUSOCBounds_18': ((data>>48)&0xFF)*0.1,                                  #
        'USUSOCBounds_19': ((data>>40)&0xFF)*0.1,                                  #
        'USUSOCBounds_20': ((data>>32)&0xFF)*0.1,                                  #
        'USUSOCBounds_21': ((data>>24)&0xFF)*0.1,                                  #
        'USUSOCBounds_22': ((data>>16)&0xFF)*0.1,                                  #
        'USUSOCBounds_23': ((data>>8)&0xFF)*0.1,                                   #
        'USUSOCBounds_24': ((data)&0xFF)*0.1                                       #
    }                                                                              #
                                                                                   #
def USUSOCBoundsGroup_4(data, id):                                                 #
    return {                                                                       #
        'USUSOCBounds_25': ((data>>56)&0xFF)*0.1,                                  #
        'USUSOCBounds_26': ((data>>48)&0xFF)*0.1,                                  #
        'USUSOCBounds_27': ((data>>40)&0xFF)*0.1,                                  #
        'USUSOCBounds_28': ((data>>32)&0xFF)*0.1,                                  #
        'USUSOCBounds_29': ((data>>24)&0xFF)*0.1,                                  #
        'USUSOCBounds_30': ((data>>16)&0xFF)*0.1,                                  #
        'USUSOCBounds_31': ((data>>8)&0xFF)*0.1,                                   #
        'USUSOCBounds_32': ((data)&0xFF)*0.1                                       #
    }                                                                              #
                                                                                   #
def USUSOCBoundsGroup_5(data, id):                                                 #
    return {                                                                       #
        'USUSOCBounds_33': ((data>>56)&0xFF)*0.1,                                  #
        'USUSOCBounds_34': ((data>>48)&0xFF)*0.1,                                  #
        'USUSOCBounds_35': ((data>>40)&0xFF)*0.1,                                  #
        'USUSOCBounds_36': ((data>>32)&0xFF)*0.1,                                  #
        'USUSOCBounds_37': ((data>>24)&0xFF)*0.1,                                  #
        'USUSOCBounds_38': ((data>>16)&0xFF)*0.1,                                  #
        'USUSOCBounds_39': ((data>>8)&0xFF)*0.1,                                   #
        'USUSOCBounds_40': ((data)&0xFF)*0.1                                       #
    }                                                                              #
                                                                                   #
def USUBoardTempGroup_1(data, id):                                                 #
    return {                                                                       #
        'USUBoardTemp_1': ((data>>56)&0xFF)-40,                                    #
        'USUBoardTemp_2': ((data>>48)&0xFF)-40,                                    #
        'USUBoardTemp_3': ((data>>40)&0xFF)-40,                                    #
        'USUBoardTemp_4': ((data>>32)&0xFF)-40,                                    #
        'USUBoardTemp_5': ((data>>24)&0xFF)-40,                                    #
        'USUBoardTemp_6': ((data>>16)&0xFF)-40,                                    #
        'USUBoardTemp_7': ((data>>8)&0xFF)-40,                                     #
        'USUBoardTemp_8': ((data)&0xFF)-40                                         #
    }                                                                              #
                                                                                   #
def USUBoardTempGroup_2(data, id):                                                 #
    return {                                                                       #
        'USUBoardTemp_9': ((data>>56)&0xFF)-40,                                    #
        'USUBoardTemp_10': ((data>>48)&0xFF)-40,                                   #
        'USUBoardTemp_11': ((data>>40)&0xFF)-40,                                   #
        'USUBoardTemp_12': ((data>>32)&0xFF)-40,                                   #
        'USUBoardTemp_13': ((data>>24)&0xFF)-40,                                   #
        'USUBoardTemp_14': ((data>>16)&0xFF)-40,                                   #
        'USUBoardTemp_15': ((data>>8)&0xFF)-40,                                    #
        'USUBoardTemp_16': ((data)&0xFF)-40                                        #
    }                                                                              #
                                                                                   #
def USUBoardTempGroup_3(data, id):                                                 #
    return {                                                                       #
        'USUBoardTemp_17': ((data>>56)&0xFF)-40,                                   #
        'USUBoardTemp_18': ((data>>48)&0xFF)-40,                                   #
        'USUBoardTemp_19': ((data>>40)&0xFF)-40,                                   #
        'USUBoardTemp_20': ((data>>32)&0xFF)-40,                                   #
        'USUBoardTemp_21': ((data>>24)&0xFF)-40,                                   #
        'USUBoardTemp_22': ((data>>16)&0xFF)-40,                                   #
        'USUBoardTemp_23': ((data>>8)&0xFF)-40,                                    #
        'USUBoardTemp_24': ((data)&0xFF)-40                                        #
    }                                                                              #
                                                                                   #
def USUBoardTempGroup_4(data, id):                                                 #
    return {                                                                       #
        'USUBoardTemp_25': ((data>>56)&0xFF)-40,                                   #
        'USUBoardTemp_26': ((data>>48)&0xFF)-40,                                   #
        'USUBoardTemp_27': ((data>>40)&0xFF)-40,                                   #
        'USUBoardTemp_28': ((data>>32)&0xFF)-40,                                   #
        'USUBoardTemp_29': ((data>>24)&0xFF)-40,                                   #
        'USUBoardTemp_30': ((data>>16)&0xFF)-40,                                   #
        'USUBoardTemp_31': ((data>>8)&0xFF)-40,                                    #
        'USUBoardTemp_32': ((data)&0xFF)-40                                        #
    }                                                                              #
                                                                                   #
def USUBoardTempGroup_5(data, id):                                                 #
    return {                                                                       #
        'USUBoardTemp_33': ((data>>56)&0xFF)-40,                                   #
        'USUBoardTemp_34': ((data>>48)&0xFF)-40,                                   #
        'USUBoardTemp_35': ((data>>40)&0xFF)-40,                                   #
        'USUBoardTemp_36': ((data>>32)&0xFF)-40,                                   #
        'USUBoardTemp_37': ((data>>24)&0xFF)-40,                                   #
        'USUBoardTemp_38': ((data>>16)&0xFF)-40,                                   #
        'USUBoardTemp_39': ((data>>8)&0xFF)-40,                                    #
        'USUBoardTemp_40': ((data)&0xFF)-40                                        #
    }                                                                              #
                                                                                   #
def USUBusVoltageGroup_1(data, id):                                                #
    return {                                                                       #
        'USULVBusVoltage_1': ((data>>48)&0xFFF)*0.01,                              #
        'USULVBusVoltage_2': ((data>>32)&0xFFF)*0.01,                              #
        'USULVBusVoltage_3': ((data>>16)&0xFFF)*0.01,                              #
        'USULVBusVoltage_4': ((data)&0xFFF)*0.01                                   #
    }                                                                              #
                                                                                   #
def USUBusVoltageGroup_2(data, id):                                                #
    return {                                                                       #
        'USULVBusVoltage_5': ((data>>48)&0xFFF)*0.01,                              #
        'USULVBusVoltage_6': ((data>>32)&0xFFF)*0.01,                              #
        'USULVBusVoltage_7': ((data>>16)&0xFFF)*0.01,                              #
        'USULVBusVoltage_8': ((data)&0xFFF)*0.01                                   #
    }                                                                              #
                                                                                   #
def USUBusVoltageGroup_3(data, id):                                                #
    return {                                                                       #
        'USULVBusVoltage_9': ((data>>48)&0xFFF)*0.01,                              #
        'USULVBusVoltage_10': ((data>>32)&0xFFF)*0.01,                             #
        'USULVBusVoltage_11': ((data>>16)&0xFFF)*0.01,                             #
        'USULVBusVoltage_12': ((data)&0xFFF)*0.01                                  #
    }                                                                              #
                                                                                   #
def USUBusVoltageGroup_4(data, id):                                                #
    return {                                                                       #
        'USULVBusVoltage_13': ((data>>48)&0xFFF)*0.01,                             #
        'USULVBusVoltage_14': ((data>>32)&0xFFF)*0.01,                             #
        'USULVBusVoltage_15': ((data>>16)&0xFFF)*0.01,                             #
        'USULVBusVoltage_16': ((data)&0xFFF)*0.01                                  #
    }                                                                              #
                                                                                   #
def USUBusVoltageGroup_5(data, id):                                                #
    return {                                                                       #
        'USULVBusVoltage_17': ((data>>48)&0xFFF)*0.01,                             #
        'USULVBusVoltage_18': ((data>>32)&0xFFF)*0.01,                             #
        'USULVBusVoltage_19': ((data>>16)&0xFFF)*0.01,                             #
        'USULVBusVoltage_20': ((data)&0xFFF)*0.01                                  #
    }                                                                              #
                                                                                   #
def USUBusVoltageGroup_6(data, id):                                                #
    return {                                                                       #
        'USULVBusVoltage_21': ((data>>48)&0xFFF)*0.01,                             #
        'USULVBusVoltage_22': ((data>>32)&0xFFF)*0.01,                             #
        'USULVBusVoltage_23': ((data>>16)&0xFFF)*0.01,                             #
        'USULVBusVoltage_24': ((data)&0xFFF)*0.01                                  #
    }                                                                              #
                                                                                   #
def USUBusVoltageGroup_7(data, id):                                                #
    return {                                                                       #
        'USULVBusVoltage_25': ((data>>48)&0xFFF)*0.01,                             #
        'USULVBusVoltage_26': ((data>>32)&0xFFF)*0.01,                             #
        'USULVBusVoltage_27': ((data>>16)&0xFFF)*0.01,                             #
        'USULVBusVoltage_28': ((data)&0xFFF)*0.01                                  #
    }                                                                              #
                                                                                   #
def USUBusVoltageGroup_8(data, id):                                                #
    return {                                                                       #
        'USULVBusVoltage_29': ((data>>48)&0xFFF)*0.01,                             #
        'USULVBusVoltage_30': ((data>>32)&0xFFF)*0.01,                             #
        'USULVBusVoltage_31': ((data>>16)&0xFFF)*0.01,                             #
        'USULVBusVoltage_32': ((data)&0xFFF)*0.01                                  #
    }                                                                              #
                                                                                   #
def USUBusVoltageGroup_9(data, id):                                                #
    return {                                                                       #
        'USULVBusVoltage_33': ((data>>48)&0xFFF)*0.01,                             #
        'USULVBusVoltage_34': ((data>>32)&0xFFF)*0.01,                             #
        'USULVBusVoltage_35': ((data>>16)&0xFFF)*0.01,                             #
        'USULVBusVoltage_36': ((data)&0xFFF)*0.01                                  #
    }                                                                              #
                                                                                   #
def USUBusVoltageGroup_10(data, id):                                               #
    return {                                                                       #
        'USULVBusVoltage_37': ((data>>48)&0xFFF)*0.01,                             #
        'USULVBusVoltage_38': ((data>>32)&0xFFF)*0.01,                             #
        'USULVBusVoltage_39': ((data>>16)&0xFFF)*0.01,                             #
        'USULVBusVoltage_40': ((data)&0xFFF)*0.01                                  #
    }                                                                              #
                                                                                   #
def USUBusVoltageGroup_11(data, id):                                               #
    return {                                                                       #
        'USULVBusVoltage_41': ((data>>48)&0xFFF)*0.01,                             #
        'USULVBusVoltage_42': ((data>>32)&0xFFF)*0.01,                             #
    }                                                                              #
                                                                                   #
def TesterFunctionalReq_H1(data, id):                                              #
    return {                                                                       #
        'TesterFunctionalReq': data                                                #
    }                                                                              #
                                                                                   #
def TesterPhysicalReqBECM(data, id):                                               #
    return {                                                                       #
        'TesterPhysicalReqBECM': data                                              #
    }                                                                              #
                                                                                   #
def TesterPhysicalResBECM(data, id):                                               #
    return {                                                                       #
        'TesterPhysicalResBECM': data                                              #
    }                                                                              #
####################################################################################

#determines how to input data into sql database
def send2SQL(dataGroup, dataHash):
    #conn = sqlite3.connect('can_data.db');
    return

#main parser script with dictionary branching to data processing function according to ID
def parse_data(line):
    pat = r'(\d+)/(\d+)/(\d+) (\d+):(\d+):(\d+)\.(\d+): (\w+) (.*) '
    match = re.match(pat, line)
    myDateTime = datetime.datetime(int(match.group(3)),int(match.group(1)),int(match.group(2)),int(match.group(4)),int(match.group(5)),int(match.group(6)),int(match.group(7))*1000)
    
    #transform 8-byte list to one 64-bit long
    data = int(''.join(match.group(9).split(' ')),16)

    #function names associated with each ID
    ID = {
        '101': ControlBatteryCmds,
        '103': HVCyclerStatus,
        '105': Battery_Traction_1,
        '109': LVMVCyclerStatus,
        '121': ControlConverterCmds,
        '122': ManualConverterCmds,
        '124': SetConverterCellLimits,
        '126': SetConverterBusLimits,
        '140': TargetStatus,
        '22A': Battery_Traction_2,
        '22B': Battery_Traction_3,
        '22C': Battery_Traction_4,
        '22D': Battery_Traction_5,
        '300': CellVoltageGroup_1,
        '301': CellVoltageGroup_2,
        '302': CellVoltageGroup_3,
        '303': CellVoltageGroup_4,
        '304': CellVoltageGroup_5,
        '305': CellVoltageGroup_6,
        '306': CellVoltageGroup_7,
        '307': CellVoltageGroup_8,
        '308': CellVoltageGroup_9,
        '309': CellVoltageGroup_10,
        '30A': CellVoltageGroup_11,
        '30B': CellVoltageGroup_12,
        '30C': CellVoltageGroup_13,
        '30D': CellVoltageGroup_14,
        '30E': CellVoltageGroup_15,
        '30F': CellVoltageGroup_16,
        '310': CellVoltageGroup_17,
        '311': CellVoltageGroup_18,
        '312': CellVoltageGroup_19,
        '313': CellVoltageGroup_20,
        '314': CellVoltageGroup_21,
        '350': CellCurrentGroup_1,
        '351': CellCurrentGroup_2,
        '352': CellCurrentGroup_3,
        '353': CellCurrentGroup_4,
        '354': CellCurrentGroup_5,
        '355': CellCurrentGroup_6,
        '356': CellCurrentGroup_7,
        '357': CellCurrentGroup_8,
        '358': CellCurrentGroup_9,
        '359': CellCurrentGroup_10,
        '35A': CellCurrentGroup_11,
        '360': BECMCellTempGroup_1,
        '361': BECMCellTempGroup_2,
        '362': BECMCellTempGroup_3,
        '363': BECMCellTempGroup_4,
        '364': BECMCellTempGroup_5,
        '365': BECMCellTempGroup_6,
        '366': BECMCellTempGroup_7,
        '367': BECMCellTempGroup_8,
        '368': BECMCellTempGroup_9,
        '369': BECMCellTempGroup_10,
        '36A': BECMCellTempGroup_11,
        '36B': BECMCellTempGroup_12,
        '36C': BECMCellTempGroup_13,
        '36D': BECMCellTempGroup_14,
        '36E': BECMCellTempGroup_15,
        '36F': BECMCellTempGroup_16,
        '370': BECMCellTempGroup_17,
        '371': BECMCellTempGroup_18,
        '372': BECMCellTempGroup_19,
        '373': BECMCellTempGroup_20,
        '374': BECMCellTempGroup_21,
        '400': CellTempGroup_1,
        '401': CellTempGroup_2,
        '402': CellTempGroup_3,
        '403': CellTempGroup_4,
        '404': CellTempGroup_5,
        '405': CellTempGroup_6,
        '406': CellTempGroup_7,
        '407': CellTempGroup_8,
        '408': CellTempGroup_9,
        '409': CellTempGroup_10,
        '40A': CellTempGroup_11,
        '40B': CellTempGroup_12,
        '40C': CellTempGroup_13,
        '40D': CellTempGroup_14,
        '40E': CellTempGroup_15,
        '40F': CellTempGroup_16,
        '410': CellTempGroup_17,
        '411': CellTempGroup_18,
        '412': CellTempGroup_19,
        '413': CellTempGroup_20,
        '414': CellTempGroup_21,
        '420': USUCellVoltageGroup_1,
        '421': USUCellVoltageGroup_2,
        '422': USUCellVoltageGroup_3,
        '423': USUCellVoltageGroup_4,
        '424': USUCellVoltageGroup_5,
        '425': USUCellVoltageGroup_6,
        '426': USUCellVoltageGroup_7,
        '427': USUCellVoltageGroup_8,
        '428': USUCellVoltageGroup_9,
        '429': USUCellVoltageGroup_10,
        '42A': USUCellVoltageGroup_11,
        '440': USUCellSOCGroup_1,
        '441': USUCellSOCGroup_2,
        '442': USUCellSOCGroup_3,
        '443': USUCellSOCGroup_4,
        '444': USUCellSOCGroup_5,
        '445': USUCellSOCGroup_6,
        '446': USUCellSOCGroup_7,
        '447': USUCellSOCGroup_8,
        '448': USUCellSOCGroup_9,
        '449': USUCellSOCGroup_10,
        '44A': USUCellSOCGroup_11,
        '461': USUSOCBoundsGroup_1,
        '462': USUSOCBoundsGroup_2,
        '463': USUSOCBoundsGroup_3,
        '464': USUSOCBoundsGroup_4,
        '465': USUSOCBoundsGroup_5,
        '481': USUBoardTempGroup_1,
        '482': USUBoardTempGroup_2,
        '483': USUBoardTempGroup_3,
        '484': USUBoardTempGroup_4,
        '485': USUBoardTempGroup_5,
        '4A1': USUBusVoltageGroup_1,
        '4A2': USUBusVoltageGroup_2,
        '4A3': USUBusVoltageGroup_3,
        '4A4': USUBusVoltageGroup_4,
        '4A5': USUBusVoltageGroup_5,
        '4A6': USUBusVoltageGroup_6,
        '4A7': USUBusVoltageGroup_7,
        '4A8': USUBusVoltageGroup_8,
        '4A9': USUBusVoltageGroup_9,
        '4AA': USUBusVoltageGroup_10,
        '4AB': USUBusVoltageGroup_11,
        '7DF': TesterFunctionalReq_H1,
        '7E4': TesterPhysicalReqBECM,
        '7EC': TesterPhysicalResBECM
    }

    func = ID.get(match.group(8), Unknown)
    return func(data,match.group(8))

with zipfile.ZipFile('12-43-24.zip','r') as zipin:
    with open ("output.txt",'w+') as outfile:
        infile = zipin.open("12-43-24.txt",'r')
        [outfile.write(str(parse_data(line))+'\n') for line in infile.readlines()]