#!/usr/bin/env python
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
import oracledb as cx
import math

# In[2]:


outputPath = r"" 
outputFile = r""
#outputPath - path where the file should be saved
#outputFile - name of file .xlsx

channels = []
#chanels - 'TOBA', 'LCON', 'HREV', 'GROC'

periodicity = 1
#1 - weekly
#2 - monthly

periodTo = 
#period
periodsBack = 
#periods back (recomended - 6MM/ -12WK)

mode = 'TAG' # TAG OR NAN

nanList = [] # FOR NAN MODE
tagList = [] 
# FOR TAG MODE
fdsList = [] # FOR TAG MODE

output = f"{outputPath}\{outputFile}"

#DICT SETUPS

periodicityDict = {1: '',
                   2: '_MM'}

charsDict = {'AC_BRAND1': 1318,
             'AC_PACKAGING': 1332,
             'NC_PNU': 1016,
             'NC_WTU': 1015,
             'NC_PIECE': 1043}

#FUNCTIONS

#BASE CONVERTER
def base2base(shortTag, cutPrefix=True, intInputBase=36, intOutputBase=10):
    result=""
    numericBaseData =""#put number of database
    maxBase=len(numericBaseData)
    if cutPrefix:
        shortTag=shortTag[1:]
    if intInputBase > maxBase or intOutputBase > maxBase:
        return result
    else:
        inputNumLength=len(shortTag)
        decValue=0
        for i in range(0,inputNumLength):
            for j in range(0,intInputBase):
                if shortTag[i] == numericBaseData[j]:
                    decValue+=math.floor(j*(intInputBase**(inputNumLength - (i+1))) + 0.5)
    while(decValue>0):
        x=math.floor(((decValue/intOutputBase) - math.floor(decValue/intOutputBase)) * intOutputBase + 1.5)
        result=numericBaseData[x-1] + result
        decValue=math.floor(decValue/intOutputBase)
    return(result)

#TAG CONVERTER APPLIED
def convertTag(fullTag):
    prefix=""
    if fullTag[0]=='L':
        prefix='1'
    elif fullTag[0]=='P':
        prefix='2'
    else:
        prefix='0'
    numCode=base2base(fullTag)
    return(f"{prefix}{int(numCode):020d}")

#TAGS - NODES TRANSLATION
def extractTags(fdsList, tagList):
    dfTemp = pd.DataFrame(tagList, columns = ['AC_TAG']).assign(NODE_ID = lambda x: x['AC_TAG'].apply(convertTag))
    conNrsp = cx.connect(r'')#NRSP Base
    dfTags = []
    for fds in fdsList:
        dfTags.append(pd.read_sql_query(r"select FDS_ID, to_char(NODE_ID) as NODE_ID, NAN_KEY as F_NAN_KEY " +
                                        r"from nrsp_v.vrag_fds_flat_hie_prd " +
                                        f"where fds_id = {fds} " +
                                        f"and node_id in ({', '.join([str(x) for x in dfTemp['NODE_ID']])})", conNrsp))
    dfTags = pd.concat(dfTags).merge(dfTemp, how = 'inner', on = 'NODE_ID')
    conNrsp.close()
    return(dfTags)
            


# In[3]:

conSirval = cx.connect(r'')#Sirval Base
try:
    #PERIODS
    dfPeriods = pd.read_sql_query(r"select AC_PERIODLABEL, NC_PERIODID " +
                                    r"from (select AC_PERIODLABEL, NC_PERIODID " +
                                    f"from vldsys_pt.periods{periodicityDict.get(periodicity)} " +
                                    f"where NC_PERIODID <= {periodTo} " +
                                    r"order by NC_PERIODID desc) " +
                                    f"where rownum <= {periodsBack}", conSirval)

    #NANS
    dfNans = None
    dfTagNans = None
    if mode == 'TAG':
        dfTagNans = extractTags(fdsList, tagList)
        dfNans = dfTagNans.loc[:, ['F_NAN_KEY']].drop_duplicates()
    else:
        dfNans = pd.DataFrame(nanList, columns = ['F_NAN_KEY']).drop_duplicates()
        dfTagNans = dfNans.copy().assign(FDS_ID = -1, NODE_ID = '-1', AC_TAG = '-1')
        dfTagNans = dfTagNans.loc[:, ['FDS_ID', 'NODE_ID', 'F_NAN_KEY', 'AC_TAG']]
        
    #RAWDATA + SAMPLE
    dfRwd = []
    dfSamp = []
    chan = ', '.join([f"'{x}'" for x in channels])
    nans = ', '.join([str(x) for x in dfNans['F_NAN_KEY']])
    i = 1
    for period in dfPeriods['NC_PERIODID']:
        dfRwd.append(pd.read_sql_query(r"select r.NC_PERIODID, s.AC_CHANNELID, s.AC_RETAILER, s.AC_SHOPTYPE, s.AC_COUNTRYID, " +
                                        r"r.AC_NSHOPID, s.AC_AREA, s.AC_SHOPSTATUS, s.NC_XF, r.AC_DTGROUP, r.NC_HASH_SIGNATURE, " +
                                        r"f.AC_CREFSUFFIX as AC_CREFDESCRIPTIONSUFFIX, " +
                                        r"r.AC_XCODEGR, r.AC_CREF, d.AC_CREFDESCRIPTION, r.F_NAN_KEY, "
                                        r"decode(r.AC_DTGROUP, 'VOLUMETRIC', round(r.NC_SLOT2/r.NC_SLOT1, 2), nvl(r.NC_SLOT3, 0)) as NC_RWD_PRICE, " +
                                        r"decode(r.AC_DTGROUP, 'VOLUMETRIC', round(r.NC_SLOT2/(r.NC_SLOT1 * nvl(decode(r.NC_CONV, 0, 1, r.NC_CONV), 1)), 2), nvl(r.NC_SLOT3, 0)) as NC_FACTPRICE, " +
                                        r"round(decode(r.AC_DTGROUP, 'VOLUMETRIC', r.NC_SLOT5), 2) as NC_REG_PRICE, " +
                                        r"r.AC_XCODEGRMATCH, r.AC_CREFSUFFIX, r.NC_CONV, r.NC_MODIFLAG, " +
                                        r"decode(r.AC_DTGROUP, 'VOLUMETRIC', r.NC_SLOT1, r.NC_SLOT4) as NC_SALES, " +
                                        r"decode(r.AC_DTGROUP, 'VOLUMETRIC', r.NC_SLOT1 * nvl(r.NC_CONV, 1), r.NC_SLOT4) as NC_FACT_SALES, " +
                                        r"decode(r.AC_DTGROUP, 'VOLUMETRIC', r.NC_SLOT2, round(r.NC_SLOT3 *  nvl(r.NC_SLOT4, 0), 2)) as NC_VALUE, " +
                                        r"decode(r.AC_DTGROUP, 'AUDIT_DTYPE', r.NC_SLOT1) as NC_STOCK, " +
                                        r"decode(r.AC_DTGROUP, 'AUDIT_DTYPE', r.NC_SLOT2) as NC_PURCHASE " +
                                        f"from vldsys_pt.rawdata{periodicityDict.get(periodicity)} r " +
                                        r"inner join vldsys_pt.stores s on r.AC_NSHOPID = s.AC_NSHOPID " +
                                        r"left join vldsys_pt.descriptions d on r.NC_HASH_SIGNATURE = d.NC_HASH_SIGNATURE and r.AC_CREF = d.AC_CREF " +
                                        r"left join vldsys_pt.descrsuffixes f on r.NC_HASH_SIGNATURE = f.NC_HASH_SIGNATURE and r.AC_XCODEGR = f.AC_XCODEGR " +
                                        r"and r.AC_CREF = f.AC_CREF and r.NC_PERIODID between f.NC_PERACTIVEFROM and f.NC_PERACTIVETO " +
                                        r"where r.AC_DTGROUP in ('AUDIT_DTYPE', 'VOLUMETRIC') " +
                                        f"and s.AC_CHANNELID in ({chan}) " +
                                        f"and r.NC_PERIODID = {period} " +
                                        f"and r.F_NAN_KEY in ({nans}) ", conSirval))
        dfSamp.append(pd.read_sql_query(r"select m.NC_PERIODID, m.AC_PSHOPN as AC_NSHOPID, m.AC_MBDS, " +
                                        r"round(max(s.NC_SXF), 2) as NC_SXF, " + 
                                        r"listagg(s.NC_SAMPLEID, ', ') within group (order by s.NC_SAMPLEID) as NC_SAMPLES " +
                                        r"from (select j.NC_PERIODID, j.AC_PSHOPN, " +
                                        r"listagg(j.AC_MBD || ': ' || l.AC_MBDLABEL, ', ') within group (order by j.AC_MBD) as AC_MBDS " +
                                        f"from vldsys_pt.prjmbdshops{periodicityDict.get(periodicity)} j " +
                                        f"inner join vldsys_pt.prjmbds{periodicityDict.get(periodicity)} l on j.AC_MBD = l.AC_MBD " +
                                        r"and j.NC_PERIODID between l.NC_PERFROM and l.NC_PERTO and j.NC_PRJINDEXID = l.NC_PRJINDEXID " +
                                        f"where j.NC_PERIODID = {period} " +
                                        r"group by j.NC_PERIODID, j.AC_PSHOPN) m " +
                                        f"inner join vldsys_pt.prjshops{periodicityDict.get(periodicity)} s on m.NC_PERIODID = s.NC_PERIODID and m.AC_PSHOPN = s.AC_PSHOPN " +
                                        r"group by m.NC_PERIODID, m.AC_PSHOPN, m.AC_MBDS", conSirval))
        print(f"PROGRESS: {round(i/periodsBack, 2)*100}% - PERIOD: {period} EXTRACTED")
        i = i + 1
    dfRwd = pd.concat(dfRwd)
    dfSamp = pd.concat(dfSamp)
    dfSamp = dfSamp.assign(NC_SXF = lambda x: x['NC_SXF'].astype('float64'))
    del chan

    #CHARS, BENCHMARKS, FACTORS
    chars = ', '.join([str(x) for x in charsDict.values()])
    dfModuleChars = pd.read_sql_query(r"select NC_SUPERGROUPID, F_SGT_SHORT_DESC, NC_MODULEID, F_MOT_SHORT_DESC, " +
                                        r"F_NAN_KEY, F_NAN_ITEM_TYPE, AC_NANDESCRSHORT, NC_PNU, NC_WTU, NC_PIECE, AC_BRAND1, AC_PACKAGING " +
                                        r"from (select m.NC_SUPERGROUPID, s.F_SGT_SHORT_DESC, m.NC_MODULEID, " +
                                        r"m.F_MOT_SHORT_DESC, v.F_NCV_NAN_KEY as F_NAN_KEY, n.AC_NANDESCRSHORT, n.F_NAN_ITEM_TYPE, " +
                                        r"c.NC_CHARID, c.AC_CHARVALUETAG " +
                                        r"from vldsys_pt.nancharvals v " +
                                        r"inner join vldsys_pt.charvals c on v.NC_CHARVALUEID = c.NC_CHARVALUEID " +
                                        r"inner join vldsys_pt.nans n on v.F_NCV_NAN_KEY = n.F_NAN_KEY " +
                                        r"inner join vldsys_pt.modules m on n.NC_MODULEID = m.NC_MODULEID " +
                                        r"inner join vldsys_pt.supergroups s on m.NC_SUPERGROUPID = s.NC_SUPERGROUPID "
                                        f"where c.NC_CHARID in (1015, 1016, 1043, 1318, 1332) and v.F_NCV_NAN_KEY in ({nans})) " +
                                        r"pivot(max(AC_CHARVALUETAG) " +
                                        f"for NC_CHARID in ('{charsDict.get('NC_PNU')}' as NC_PNU, '{charsDict.get('NC_WTU')}' as NC_WTU, '{charsDict.get('NC_PIECE')}' as NC_PIECE, " +
                                        f"'{charsDict.get('AC_BRAND1')}' as AC_BRAND1, '{charsDict.get('AC_PACKAGING')}' as AC_PACKAGING)) ", conSirval)
    dfBench = pd.read_sql_query(r"select to_number(AC_ITEMKEY) as F_NAN_KEY, AC_STOREGROUP, " +
                                r"round(NC_PRICE_MEAN, 2) as NC_PRICE_MEAN, " +
                                r"min(round(NC_PRICE_MEAN, 2)) over (partition by AC_ITEMKEY) as NC_MIN_PRICE_MEAN, " +
                                r"max(round(NC_PRICE_MEAN, 2)) over (partition by AC_ITEMKEY) as NC_MAX_PRICE_MEAN " +
                                r"from vldsys_pt.ibenchmarks " +
                                f"where AC_ITEMTYPE = '_NAN_' and to_number(AC_ITEMKEY) in ({nans})", conSirval)
    dfFactors = pd.read_sql_query(r"select F_NAN_KEY, listagg(replace(NC_CONV, ',', '.'), ', ') within group (order by NC_CONV) as NC_FACTORS " +
                                    r"from (select F_XCI_NAN_KEY as F_NAN_KEY, NC_CONV " +
                                    r"from vldsys_pt.xcodes " +
                                    f"where F_XCI_XCODE_KEY <= 0 and F_XCI_NAN_KEY in ({nans}) " +
                                    r"group by F_XCI_NAN_KEY, NC_CONV) " +
                                    r"group by F_NAN_KEY", conSirval)
    del chars, nans

    dfRwd = dfRwd.assign(AC_STOREGROUP = lambda x: x['AC_COUNTRYID'] + ':' + x['AC_SHOPTYPE']).                 merge(dfPeriods, how = 'inner', on = 'NC_PERIODID').                 merge(dfSamp, how = 'left', on = ['NC_PERIODID', 'AC_NSHOPID']).                 merge(dfModuleChars, how = 'inner', on = 'F_NAN_KEY').                 merge(dfFactors, how = 'left', on = 'F_NAN_KEY').                 merge(dfBench, how = 'left', on = ['F_NAN_KEY', 'AC_STOREGROUP']).                 merge(dfTagNans.groupby('F_NAN_KEY')['AC_TAG'].apply(list).reset_index().rename(columns = {'AC_TAG': 'AC_TAGS'}),
                        how = 'inner', on = 'F_NAN_KEY')
    dfRwd = dfRwd.assign(NC_EXPSALES = lambda x: round(x['NC_FACT_SALES'] * np.where(pd.isna(x['NC_SXF']), x['NC_XF'], x['NC_SXF']), 2), 
                            NC_EXPVALUE = lambda x: round(x['NC_VALUE'] * np.where(pd.isna(x['NC_SXF']), x['NC_XF'], x['NC_SXF']), 2),
                            AC_COMMENTS = "")
    dfRwd = dfRwd.loc[:, ['AC_PERIODLABEL', 'NC_PERIODID', 'AC_CHANNELID', 'AC_RETAILER', 
                            'AC_STOREGROUP', 'AC_NSHOPID', 'AC_AREA', 'AC_MBDS', 'AC_SHOPSTATUS', 'NC_SAMPLES', 'NC_SXF',
                            'AC_DTGROUP', 'NC_HASH_SIGNATURE', 'AC_CREFDESCRIPTIONSUFFIX',
                            'AC_XCODEGR', 'AC_CREF', 'AC_CREFDESCRIPTION', 
                            'NC_SUPERGROUPID', 'F_SGT_SHORT_DESC', 'NC_MODULEID', 'F_MOT_SHORT_DESC',
                            'AC_TAGS', 'F_NAN_KEY', 'F_NAN_ITEM_TYPE', 'AC_NANDESCRSHORT',
                            'NC_PNU', 'NC_WTU', 'NC_PIECE', 'AC_BRAND1', 'AC_PACKAGING',
                            'NC_RWD_PRICE', 'NC_FACTPRICE', 'NC_REG_PRICE', 'NC_PRICE_MEAN', 'AC_COMMENTS',
                            'AC_XCODEGRMATCH', 'AC_CREFSUFFIX', 'NC_CONV', 'NC_MODIFLAG', 'NC_SALES', 'NC_FACT_SALES',
                            'NC_VALUE', 'NC_STOCK', 'NC_PURCHASE', 'NC_FACTORS', 'NC_MIN_PRICE_MEAN', 'NC_MAX_PRICE_MEAN',
                            'NC_EXPSALES', 'NC_EXPVALUE']]
    with pd.ExcelWriter(output) as excelWriter:
        dfTagNans.to_excel(excelWriter, "TAGS CONVERSION", index = False)
        dfRwd.to_excel(excelWriter, "DATA", index = False)
    print(f"DONE, REPORT EXTRACTED TO {output}")

except:
    print("Error has occured!")
finally:
    conSirval.close()


# %%
