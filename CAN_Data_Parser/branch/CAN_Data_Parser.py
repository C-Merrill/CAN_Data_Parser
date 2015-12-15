#!/usr/bin/python

####################################################################################
##  CAN Data Parser for 
##  

import zipfile
import mysql.connector
import re
import sys
from datetime import datetime

#Data processing functions by ID that return a dictionary with the data ID and value
####################################################################################
def ControlBatteryCmds(data, id):                                                 ##
    return {                                                                      ##
        'FanSwitch': (data>>63)&0x1,                                              ##
        'BattTracCnnct_D_Rq': (data>>43)&0x3,                                     ##
        'CellBalSwitch_Expd': (data>>41)&0x3                                      ##
    }                                                                             ##
                                                                                  ##
def HVCyclerStatus(data, id):                                                     ##
    return {                                                                      ##
        'HV_Cycler_on_status': (data>>63)&0x1,                                    ##
        'HV_Cycler_on_cmd': (data>>62)&0x1,                                       ##
        'HV_Cycler_Power_cmd': ((data>>48)&0x7FF)*100-102400,                     ##
        'HV_Cycler_Current_Actl': ((data>>32)&0x7FFF)*0.05-750,                   ##
        'HV_Cycler_Voltage_Actl': ((data>>8)&0xFFF)*0.1                           ##
    }                                                                             ##
                                                                                  ##
def Battery_Traction_1(data, id):                                                 ##
    return {                                                                      ##
        'BattTracCnnct_B_Cmd': (data>>63)&0x1,                                    ##
        'BattTrac_I_Actl': ((data>>48)&0x7FFF)*0.05-750,                          ##
        'BattTracOff_B_Actl': (data>>44)&0x1,                                     ##
        'BattTracMil_D_Rq': (data>>42)&0x3,                                       ##
        'BattTrac_U_Actl': ((data>>32)&0x3FF)*0.5,                                ##
        'BattTrac_U_LimHi': ((data>>24)&0xFF)*2,                                  ##
        'BattTrac_U_LimLo': ((data>>16)&0xFF)*2                                   ##
    }                                                                             ##
                                                                                  ##
def LVMVCyclerStatus(data, id):                                                   ##
    return {                                                                      ##
        'LV_Cycler_cmd': (data>>62)&0x3,                                          ##
        'LV_Cycler_Current': ((data>>48)&0x7FF)*0.05-20,                          ##
        'LV_Cycler_Voltage': ((data>>40)&0xFF)*0.1,                               ##
        'MV_Cycler_cmd': (data>>30)&0x3,                                          ##
        'MV_Cycler_status': (data>>28)&0x3,                                       ##
        'MV_Cycler_Current': ((data>>16)&0x7FF)*0.05-20,                          ##
        'MV_Cycler_Voltage': (data>>8)&0xFF                                       ##
    }                                                                             ##
                                                                                  ##
def ControlConverterCmds(data, id):                                               ##
    return {                                                                      ##
        'ConverterEnable': (data>>63)&0x1,                                        ##
        'ConverterCalibrate': (data>>61)&0x3,                                     ##
        'CalibrationVoltage': ((data>>40)&0x1FFF)*0.0001+10,                      ##
        'Objective_Map_Rq': (data>>37)&0x7,                                       ##
        'Objective_Map_LifeGain': ((data>>24)&0xFF)*0.0001                        ##
    }                                                                             ##
                                                                                  ##
def ManualConverterCmds(data, id):                                                ##
    return {                                                                      ##
        'ConverterEnableSampling': (data>>62)&0x3,                                ##
        'ConverterEnableSwitching': (data>>60)&0x3,                               ##
        'ConverterOpenClosedLoop': (data>>58)&0x3,                                ##
        'Phase_rq': (data>>32)&0xFFFFFF,                                          ##
        'DestinationAddress': (data>>24)&0xFF,                                    ##
        'DestinationAddressCap': (data>>16)&0xFF,                                 ##
        'CellCapacity': (data&0x7FF)*0.01+7                                       ##
    }                                                                             ##
                                                                                  ##
def SetConverterCellLimits(data, id):                                             ##
    return {                                                                      ##
        'HighCellVoltageLimit': ((data>>48)&0xFFFF)*0.0001,                       ##
        'LowCellVoltageLimit': ((data>>32)&0xFFFF)*0.0001,                        ##
        'HighCellCurrentLimit': ((data>>16)&0xFFFF)*0.001-32.765,                 ##
        'LowCellCurrentLimit': ((data)&0xFFFF)*0.001-32.765                       ##
    }                                                                             ##
                                                                                  ##
def SetConverterBusLimits(data, id):                                              ##
    return {                                                                      ##
        'HighBusVoltageLimit': ((data>>48)&0xFFFF)*0.001,                         ##
        'LowBusVoltageLimit': ((data>>32)&0xFFFF)*0.001                           ##
    }                                                                             ##
                                                                                  ##
def TargetStatus(data, id):                                                       ##
    return {                                                                      ##
        'System_Mode': (data>>62)&0x3,                                            ##
        'Operating_Mode': (data>>59)&0x7,                                         ##
        'Objective_Map_Actl': (data>>56)&0x7,                                     ##
        'Status_Normal': (data>>55)&0x1,                                          ##
        'Status_System_Fault': (data>>54)&0x1,                                    ##
        'Status_Saturation': (data>>53)&0x1,                                      ##
        'Status_Comm_Fault': (data>>52)&0x1,                                      ##
        'Status_Converter_Fault': (data>>51)&0x1,                                 ##
        'Comm_Uptime_Percent': ((data>>40)&0x3FF)*0.1                             ##
    }                                                                             ##
                                                                                  ##
def Battery_Traction_2(data, id):                                                 ##
    return {                                                                      ##
        'BattTrac_Min_CellVolt': ((data>>48)&0xFFFF)*0.0001,                      ##
        'BattTrac_Max_CellVolt': ((data>>32)&0xFFFF)*0.0001,                      ##
        'BattTrac_Pw_LimChrg': ((data>>16)&0x3FF)*250,                            ##
        'BattTrac_Pw_LimDchrg': ((data)&0x3FF)*250                                ##
    }                                                                             ##
                                                                                  ##
def Battery_Traction_3(data, id):                                                 ##
    return {                                                                      ##
        'BattTracWarnLamp_B_Rq': (data>>59)&0x1,                                  ##
        'BattTracSrvcRqd_B_Rq': (data>>58)&0x1,                                   ##
        'BattTrac_Min_CellTemp': ((data>>48)&0x3FF)*0.5-50,                       ##
        'BattTrac_Max_CellTemp': ((data>>32)&0x3FF)*0.5-50,                       ##
        'BattTracSoc_Pc_MnPrtct': ((data>>16)&0x3FF)*0.1,                         ##
        'BattTracSoc_Pc_MxPrtct': ((data)&0x3FF)*0.1                              ##
    }                                                                             ##
                                                                                  ##
def Battery_Traction_4(data, id):                                                 ##
    return {                                                                      ##
        'BattTracClntIn_Te_Actl': ((data>>56)&0xFF)-50,                           ##
        'BattTracCool_D_Falt': (data>>50)&0x3,                                    ##
        'BattTrac_Te_Actl': ((data>>40)&0x3FF)*0.5-50,                            ##
        'BattTracSoc2_Pc_Actl': ((data>>24)&0x3FFF)*0.01,                         ##
        'HvacAir_Flw_EstBatt': ((data>>16)&0xFF)*0.5,                             ##
        'BattTracSoc_Pc_Dsply': ((data>>8)&0xFF)*0.5                              ##
    }                                                                             ##
                                                                                  ##
def Battery_Traction_5(data, id):                                                 ##
    return {                                                                      ##
        'BattTracSoc_Min_UHP': ((data>>48)&0x3FF)*0.1,                            ##
        'BattTracSoc_Max_UHP': ((data>>32)&0x3FF)*0.1,                            ##
        'BattTracSoc_Min_LHP': ((data>>16)&0x3FF)*0.1,                            ##
        'BattTracSoc_Max_LHP': ((data)&0x3FF)*0.1                                 ##
    }                                                                             ##
                                                                                  ##
def Unknown(data, id):                                                            ##
    return {                                                                      ##
        id: data                                                                  ##
    }                                                                             ##
                                                                                  ##
def CellVoltageGroup(data, id):                                                   ##
    r = (int(id, 16) % int('300', 16))*4                                          ##
    return {                                                                      ##
        'CellVoltage_'+str(r+1): ((data>>48)&0xFFFF)*0.0001,                      ##
        'CellVoltage_'+str(r+2): ((data>>32)&0xFFFF)*0.0001,                      ##
        'CellVoltage_'+str(r+3): ((data>>16)&0xFFFF)*0.0001,                      ##
        'CellVoltage_'+str(r+4): ((data)&0xFFFF)*0.0001                           ##
    }                                                                             ##
                                                                                  ##
def CellCurrentGroup(data, id):                                                   ##
    r = (int(id, 16) % int('350', 16))*4                                          ##
    return {                                                                      ##
        'CellCurrent_'+str(r+1): ((data>>48)&0xFFFF)*0.001-32.765,                ##
        'CellCurrent_'+str(r+2): ((data>>32)&0xFFFF)*0.001-32.765,                ##
        'CellCurrent_'+str(r+3): ((data>>16)&0xFFFF)*0.001-32.765,                ##
        'CellCurrent_'+str(r+4): ((data)&0xFFFF)*0.001-32.765                     ##
    }                                                                             ##
                                                                                  ##
def CellCurrentGroup_11(data, id):                                                ##
    return {                                                                      ##
        'CellCurrent_41': ((data>>48)&0xFFFF)*0.001-32.765,                       ##
        'CellCurrent_42': ((data>>32)&0xFFFF)*0.001-32.765                        ##
    }                                                                             ##
                                                                                  ##
def BECMCellTempGroup(data, id):                                                  ##
    r = (int(id, 16) % int('360', 16))*4                                          ##
    return {                                                                      ##
        'BECM_CellTemp_'+str(r+1): ((data>>48)&0x7FF)*0.1-40,                     ##
        'BECM_CellTemp_'+str(r+2): ((data>>32)&0x7FF)*0.1-40,                     ##
        'BECM_CellTemp_'+str(r+3): ((data>>16)&0x7FF)*0.1-40,                     ##
        'BECM_CellTemp_'+str(r+4): ((data)&0x7FF)*0.1-40                          ##
    }                                                                             ##
                                                                                  ##
def CellTempGroup(data, id):                                                      ##
    if int(id,16) < int('40E',16):                                                ##
        r = (int(id, 16) % int('400', 16))*4                                      ##
        return {                                                                  ##
            'CellTemp_'+str(r+1): ((data>>48)&0x7FF)*0.1-40,                      ##
            'CellTemp_'+str(r+2): ((data>>32)&0x7FF)*0.1-40,                      ##
            'CellTemp_'+str(r+3): ((data>>16)&0x7FF)*0.1-40,                      ##
            'CellTemp_'+str(r+4): ((data)&0x7FF)*0.1-40                           ##
        }                                                                         ##
    else:                                                                         ##
        r = ((int(id, 16) % int('400', 16))-14)*4                                 ##
        return {                                                                  ##
            'BoardTemp_'+str(r+1): ((data>>48)&0x7FF)*0.1-40,                     ##
            'BoardTemp_'+str(r+2): ((data>>32)&0x7FF)*0.1-40,                     ##
            'BoardTemp_'+str(r+3): ((data>>16)&0x7FF)*0.1-40,                     ##
            'BoardTemp_'+str(r+4): ((data)&0x7FF)*0.1-40                          ##
        }                                                                         ##                                                               
                                                                                  ##                                                                        #
def USUCellVoltageGroup(data, id):                                                ##
    r = (int(id, 16) % int('420', 16))*4                                          ##
    return {                                                                      ##
        'USUCellVoltage_'+str(r+1): ((data>>48)&0xFFFF)*0.0001,                   ##
        'USUCellVoltage_'+str(r+2): ((data>>32)&0xFFFF)*0.0001,                   ##
        'USUCellVoltage_'+str(r+3): ((data>>16)&0xFFFF)*0.0001,                   ##
        'USUCellVoltage_'+str(r+4): ((data)&0xFFFF)*0.0001                        ##
    }                                                                             ##
                                                                                  ##
def USUCellVoltageGroup_11(data, id):                                             ##
    return {                                                                      ##
        'USUCellVoltage_41': ((data>>48)&0xFFFF)*0.0001,                          ##
        'USUCellVoltage_42': ((data>>32)&0xFFFF)*0.0001                           ##
    }                                                                             ##
                                                                                  ##
def USUCellSOCGroup(data, id):                                                    ##
    r = (int(id,16) % int('440',16))*4                                            ##
    return {                                                                      ##
        'USUCellSOC_'+str(r+1): ((data>>48)&0x3FFF)*0.01,                         ##
        'USUCellSOC_'+str(r+2): ((data>>32)&0x3FFF)*0.01,                         ##
        'USUCellSOC_'+str(r+3): ((data>>16)&0x3FFF)*0.01,                         ##
        'USUCellSOC_'+str(r+4): ((data)&0x3FFF)*0.01                              ##
    }                                                                             ##
                                                                                  ##
def USUCellSOCGroup_11(data, id):                                                 ##
    return {                                                                      ##
        'USUCellSOC_41': ((data>>48)&0x3FFF)*0.01,                                ##
        'USUCellSOC_42': ((data>>32)&0x3FFF)*0.01,                                ##
        'USUSOCBounds_41': ((data>>24)&0xFF)*0.1,                                 ##
        'USUSOCBounds_42': ((data>>16)&0xFF)*0.1,                                 ##
        'USUBoardTemp_41': ((data>>8)&0xFF)-40,                                   ##
        'USUBoardTemp_42': ((data)&0xFF)-40                                       ##
    }                                                                             ##
                                                                                  ##
def USUSOCBoundsGroup(data, id):                                                  ##
    r = ((int(id,16) % int('460'))-1)*8                                           ##
    return {                                                                      ##
        'USUSOCBounds_1': ((data>>56)&0xFF)*0.1,                                  ##
        'USUSOCBounds_2': ((data>>48)&0xFF)*0.1,                                  ##
        'USUSOCBounds_3': ((data>>40)&0xFF)*0.1,                                  ##
        'USUSOCBounds_4': ((data>>32)&0xFF)*0.1,                                  ##
        'USUSOCBounds_5': ((data>>24)&0xFF)*0.1,                                  ##
        'USUSOCBounds_6': ((data>>16)&0xFF)*0.1,                                  ##
        'USUSOCBounds_7': ((data>>8)&0xFF)*0.1,                                   ##
        'USUSOCBounds_8': ((data)&0xFF)*0.1                                       ##
    }                                                                             ##
                                                                                  ##
def USUBoardTempGroup(data, id):                                                  ##
    r = ((int(id,16) % int('480',16))-1)*8                                        ##
    return {                                                                      ##
        'USUBoardTemp_1': ((data>>56)&0xFF)-40,                                   ##
        'USUBoardTemp_2': ((data>>48)&0xFF)-40,                                   ##
        'USUBoardTemp_3': ((data>>40)&0xFF)-40,                                   ##
        'USUBoardTemp_4': ((data>>32)&0xFF)-40,                                   ##
        'USUBoardTemp_5': ((data>>24)&0xFF)-40,                                   ##
        'USUBoardTemp_6': ((data>>16)&0xFF)-40,                                   ##
        'USUBoardTemp_7': ((data>>8)&0xFF)-40,                                    ##
        'USUBoardTemp_8': ((data)&0xFF)-40                                        ##
    }                                                                             ##
                                                                                  ##
def USUBusVoltageGroup(data, id):                                                 ##
    r = ((int(id,16) % int('4A0',16))-1)*4                                        ##
    return {                                                                      ##
        'USULVBusVoltage_1': ((data>>48)&0xFFF)*0.01,                             ##
        'USULVBusVoltage_2': ((data>>32)&0xFFF)*0.01,                             ##
        'USULVBusVoltage_3': ((data>>16)&0xFFF)*0.01,                             ##
        'USULVBusVoltage_4': ((data)&0xFFF)*0.01                                  ##
    }                                                                             ##
                                                                                  ##
def USUBusVoltageGroup_11(data, id):                                              ##
    return {                                                                      ##
        'USULVBusVoltage_41': ((data>>48)&0xFFF)*0.01,                            ##
        'USULVBusVoltage_42': ((data>>32)&0xFFF)*0.01,                            ##
    }                                                                             ##
                                                                                  ##
def TesterFunctionalReq_H1(data, id):                                             ##
    return {                                                                      ##
        'TesterFunctionalReq': data                                               ##
    }                                                                             ##
                                                                                  ##
def TesterPhysicalReqBECM(data, id):                                              ##
    return {                                                                      ##
        'TesterPhysicalReqBECM': data                                             ##
    }                                                                             ##
                                                                                  ##
def TesterPhysicalResBECM(data, id):                                              ##
    return {                                                                      ##
        'TesterPhysicalResBECM': data                                             ##
    }                                                                             ##
####################################################################################

#generates and executes SQL script to write data to database
def send2SQL(timestamp, dataGroup, dataHash):
    #if (dataGroup == "Unknown"):
    #    return
    feed = []
    for key, val in dataHash.iteritems():
        feed.append((dataGroup, key, timestamp, val))
    for toop in feed:
        curs.execute("INSERT INTO can_data VALUES (%s,%s,%s,%s)", toop)
    return

#main parser script with dictionary branching to data processing function according to ID
def parse_data(line):
    pat = r'(\d+)/(\d+)/(\d+) (\d+):(\d+):(\d+)\.(\d+): (\w+) (.*) '
    match = re.match(pat, line)
    myDateTime = datetime(int(match.group(3)),int(match.group(1)),int(match.group(2)),int(match.group(4)),int(match.group(5)),int(match.group(6)),int(match.group(7))*1000)
    
    #transform 8-byte data list to one 64-bit long
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
        '300': CellVoltageGroup,
        '301': CellVoltageGroup,
        '302': CellVoltageGroup,
        '303': CellVoltageGroup,
        '304': CellVoltageGroup,
        '305': CellVoltageGroup,
        '306': CellVoltageGroup,
        '307': CellVoltageGroup,
        '308': CellVoltageGroup,
        '309': CellVoltageGroup,
        '30A': CellVoltageGroup,
        '30B': CellVoltageGroup,
        '30C': CellVoltageGroup,
        '30D': CellVoltageGroup,
        '30E': CellVoltageGroup,
        '30F': CellVoltageGroup,
        '310': CellVoltageGroup,
        '311': CellVoltageGroup,
        '312': CellVoltageGroup,
        '313': CellVoltageGroup,
        '314': CellVoltageGroup,
        '350': CellCurrentGroup,
        '351': CellCurrentGroup,
        '352': CellCurrentGroup,
        '353': CellCurrentGroup,
        '354': CellCurrentGroup,
        '355': CellCurrentGroup,
        '356': CellCurrentGroup,
        '357': CellCurrentGroup,
        '358': CellCurrentGroup,
        '359': CellCurrentGroup,
        '35A': CellCurrentGroup_11,
        '360': BECMCellTempGroup,
        '361': BECMCellTempGroup,
        '362': BECMCellTempGroup,
        '363': BECMCellTempGroup,
        '364': BECMCellTempGroup,
        '365': BECMCellTempGroup,
        '366': BECMCellTempGroup,
        '367': BECMCellTempGroup,
        '368': BECMCellTempGroup,
        '369': BECMCellTempGroup,
        '36A': BECMCellTempGroup,
        '36B': BECMCellTempGroup,
        '36C': BECMCellTempGroup,
        '36D': BECMCellTempGroup,
        '36E': BECMCellTempGroup,
        '36F': BECMCellTempGroup,
        '370': BECMCellTempGroup,
        '371': BECMCellTempGroup,
        '372': BECMCellTempGroup,
        '373': BECMCellTempGroup,
        '374': BECMCellTempGroup,
        '400': CellTempGroup,
        '401': CellTempGroup,
        '402': CellTempGroup,
        '403': CellTempGroup,
        '404': CellTempGroup,
        '405': CellTempGroup,
        '406': CellTempGroup,
        '407': CellTempGroup,
        '408': CellTempGroup,
        '409': CellTempGroup,
        '40A': CellTempGroup,
        '40B': CellTempGroup,
        '40C': CellTempGroup,
        '40D': CellTempGroup,
        '40E': CellTempGroup,
        '40F': CellTempGroup,
        '410': CellTempGroup,
        '411': CellTempGroup,
        '412': CellTempGroup,
        '413': CellTempGroup,
        '414': CellTempGroup,
        '420': USUCellVoltageGroup,
        '421': USUCellVoltageGroup,
        '422': USUCellVoltageGroup,
        '423': USUCellVoltageGroup,
        '424': USUCellVoltageGroup,
        '425': USUCellVoltageGroup,
        '426': USUCellVoltageGroup,
        '427': USUCellVoltageGroup,
        '428': USUCellVoltageGroup,
        '429': USUCellVoltageGroup,
        '42A': USUCellVoltageGroup_11,
        '440': USUCellSOCGroup,
        '441': USUCellSOCGroup,
        '442': USUCellSOCGroup,
        '443': USUCellSOCGroup,
        '444': USUCellSOCGroup,
        '445': USUCellSOCGroup,
        '446': USUCellSOCGroup,
        '447': USUCellSOCGroup,
        '448': USUCellSOCGroup,
        '449': USUCellSOCGroup,
        '44A': USUCellSOCGroup_11,
        '461': USUSOCBoundsGroup,
        '462': USUSOCBoundsGroup,
        '463': USUSOCBoundsGroup,
        '464': USUSOCBoundsGroup,
        '465': USUSOCBoundsGroup,
        '481': USUBoardTempGroup,
        '482': USUBoardTempGroup,
        '483': USUBoardTempGroup,
        '484': USUBoardTempGroup,
        '485': USUBoardTempGroup,
        '4A1': USUBusVoltageGroup,
        '4A2': USUBusVoltageGroup,
        '4A3': USUBusVoltageGroup,
        '4A4': USUBusVoltageGroup,
        '4A5': USUBusVoltageGroup,
        '4A6': USUBusVoltageGroup,
        '4A7': USUBusVoltageGroup,
        '4A8': USUBusVoltageGroup,
        '4A9': USUBusVoltageGroup,
        '4AA': USUBusVoltageGroup,
        '4AB': USUBusVoltageGroup_11,
        '7DF': TesterFunctionalReq_H1,
        '7E4': TesterPhysicalReqBECM,
        '7EC': TesterPhysicalResBECM
    }

    func = ID.get(match.group(8), Unknown)
    dataHash = func(data,match.group(8))
    #funcname = re.search(r'function (\w*) at',str(func)).group(1)
    #if (re.match(r'\_\d', funcname)):
    #    dataGroup = funcname
    #else:
    #    iddiff = int(match.group(8),16)%16
    #    dataGroup = funcname + "_" + str(iddiff+1)
    #send2SQL(myDateTime, dataGroup, dataHash, curs)
    send2SQL(myDateTime, re.search(r'function (\w*) at',str(func)).group(1), dataHash)
    return

if __name__ == '__main__':
    zipFileName = sys.argv[1];
    start = datetime.now()
    
    conn = mysql.connector.connect(user = 'root', password = 'UPEL@usu670', database = "can_data")
    curs = conn.cursor()
    
    with zipfile.ZipFile(zipFileName,'r') as zipin:
        txtFile = zipin.namelist()[0];
        with zipin.open(txtFile,'r') as infile:
            [parse_data(line) for line in infile.readlines()]
    
    conn.commit()
    curs.close()
    conn.close()
    
    end = datetime.now()
    timer = end-start
    print timer