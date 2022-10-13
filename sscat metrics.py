#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import datetime as dt
from itertools import product




# In[3]:

pip install pymysql

# In[3]:


pip install sshtunnel


# In[4]:


pip install https://github.com/nithinmurali/pygsheets/archive/master.zip


# In[5]:


import pygsheets


# In[6]:


from sshtunnel import SSHTunnelForwarder
import pymysql as db


# In[13]:


#this query fetch data upto last month's end
end_date_for_query_filter = str(dt.date.today() - dt.timedelta(days=dt.date.today().day))
# start_date_for_query_filter = end_date_for_query_filter[:-2] + "01" #if only last month data is needed
start_date_for_query_filter = '{}-01-01'.format(str(dt.datetime.today().year-2))


# ssh variables
host = '3.110.253.174'
localhost = '127.0.0.1'
ssh_username = 'ubuntu'
ssh_private_key = r'C:\Users\shivs\OneDrive\Documents/bintix_v3.pem'

# database variables
user='readonly'
password='5nxRhkXBZUdMmqSH'
database='bintixdb_v5'

from sshtunnel import SSHTunnelForwarder
import pymysql as db

def query(q):
    with SSHTunnelForwarder(
        (host, 22),
        ssh_username=ssh_username,
        ssh_private_key=ssh_private_key,
        remote_bind_address=(localhost, 3306)
    ) as server:
        conn = db.connect(host=localhost,
                               port=server.local_bind_port,
                               user=user,
                               passwd=password,
                               db=database)

        return pd.read_sql_query(q, conn)
query_string = """select

sub1.city_id,
sub1.bintix_uid,
sub1.pickup_date,
sub1.quantity,
sub1.ean as barcode,

sub2.price,

sub2.cat,
sub2.scat,
sub2.sscat,

sub2.brand,
sub2.sub_brand,
sub2.variant,
sub2.title,

sub2.attribute1,
sub2.attribute2,
sub2.attribute3,

sub2.std_unit_value,
sub2.std_unit,
sub2.pack_of

from(select
p.community_id,
p.hierarchy_id as city_id,
p.user_id as bintix_uid,
p.pickup_date,
di.ean,
count(di.ean) as quantity
from pickup_bags pb
inner join pickups p on p.id = pb.pickup_id
inner join datalabs_processed_bags db on db.pickup_bag_id = pb.id
inner join `datalabs_processed_bag_items` di on di.`processed_bag_id` = db.id
where di.ean is not null and p.pickup_date >='{}' and p.pickup_date <='{}' and p.pickup_status ='complete' and p.community_id not in (51,60,84,333,348) and p.hierarchy_id in (1,2,3,4,5,6)
group by di.processed_bag_id,di.ean
order by di.processed_bag_id ) as sub1
left join (select
dc.barcode,
dc.name as title,
dbs.name as brand,
ds.name as sub_brand,
dc.variant,
dc.promo,
dc.type,
dc.retail_source,
dc.attribute1,
dc.attribute2,
dc.attribute3,
dm.name as manufacturer,
dc.man_classification as man_class,
dc.price as price,
cat.name AS cat,
scat.name AS scat,
sscat.name AS sscat,
dc.weight as unit_value,
dc.unit,
dc.std_unit_value,
dc.std_unit,
dc.pack_of
from datalabs_catalogs dc
left join datalabs_brands dbs on dbs.id = dc.brand_id
left join datalabs_sub_brands ds on ds.id = dc.sub_brand_id
left join datalabs_manufacturers dm on dm.id = dc.manufacturer_id
left join datalabs_categories sscat ON sscat.id = dc.category_id
left join datalabs_categories scat ON sscat.parent_id = scat.id
left join datalabs_categories cat ON scat.parent_id = cat.id
where dc.id in (
select max(id) from datalabs_catalogs group by barcode
)) as sub2 on sub1.ean = sub2.barcode""".format(start_date_for_query_filter,end_date_for_query_filter)

dff= query(query_string)


# query_string


# In[14]:


dff["pickup_date"]  = pd.to_datetime(dff.pickup_date)
dff["month_adj"] = dff.pickup_date.dt.month + (dff.pickup_date.dt.year-2019)*12
dff["year"] = dff.pickup_date.dt.year
dff["std_unit_value"] =  pd.to_numeric(dff.std_unit_value, errors='coerce')
dff["price"] =  pd.to_numeric(dff.price, errors='coerce')
dff["pack_of"] =  pd.to_numeric(dff.pack_of, errors='coerce')
dff["volume"] = dff.quantity*dff.std_unit_value
dff["binvalue"] = dff.quantity*dff.price
dff["quantity_pack"] = dff.quantity*dff.pack_of
dff.loc[dff.city_id==1,"city"] = "Hyderabad"
dff.loc[dff.city_id==2,"city"] = "Bangalore"
dff.loc[dff.city_id==3,"city"] = "Delhi"
dff.loc[dff.city_id==4,"city"] = "Mumbai"
dff.loc[dff.city_id==5,"city"] = "Kolkata"
dff.loc[dff.city_id==6,"city"] = "Chennai"

df = pd.DataFrame()
for m in dff.month_adj.unique():
    temp = dff.loc[dff.month_adj==m]
    sumq = temp.groupby("bintix_uid").quantity.sum().reset_index()
    uid_valid = sumq.loc[sumq.quantity>=30].bintix_uid.unique()
    df = pd.concat([df,temp.loc[temp.bintix_uid.isin(uid_valid)]])
df


# In[22]:


sheet_id = "1DQVt5TAd5mgN9ufToJqLiY8VebMzDPCQdQ33GiRKa5w"
sheet_name = "Sheet1"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
tghh = pd.read_csv(url)
df["tghh"] = df.city
for month_for_analysis in df.month_adj.unique():
    tghh_new_month_dict = tghh.loc[tghh.month_adj==month_for_analysis].to_dict("records")[0]
    df.loc[df.month_adj==month_for_analysis,"tghh"] = df.loc[df.month_adj==month_for_analysis,"tghh"].replace(tghh_new_month_dict)
df["tghh"] =  pd.to_numeric(df.tghh, errors='coerce')
df.dtypes


# In[23]:



def list_of_month(time_period):
    return list(range(time_period[0],time_period[1]+1))
# list_of_months([2,10])
def average_tghh_calculator(data,time_period,city_list): #for a city or group of cities
    return data.loc[(data.month_adj.isin(list_of_month(time_period))) & (data.city_id.isin(city_list))].groupby(["city_id","month_adj"]).tghh.first().reset_index().groupby('city_id').tghh.mean().sum()
# average_tghh_calculator(df,[28,30],[1,2,3,4,5,6])
def unique_bintix_uid_count(data,time_period,city_list):
    return len(data.loc[(data.month_adj.isin(list_of_month(time_period))) & (data.city_id.isin(city_list))].bintix_uid.unique())
# unique_bintix_uid_count(df,[28,30],[1,2,3,4,5,6])  
def factor_generator(data,time_period,city):
    return average_tghh_calculator(data,time_period,[city])/unique_bintix_uid_count(data,time_period,[city])  
# factor_generator(df,[28,30],1)  


# In[24]:


df["projectedfactor_monthlyv1"] = df.city_id
for month_for_analysis in  df.month_adj.unique():
    monthly_factor_dict = {
               1:factor_generator(df,[month_for_analysis,month_for_analysis],1),
               2:factor_generator(df,[month_for_analysis,month_for_analysis],2),
               3:factor_generator(df,[month_for_analysis,month_for_analysis],3),
               4:factor_generator(df,[month_for_analysis,month_for_analysis],4),
               5:factor_generator(df,[month_for_analysis,month_for_analysis],5),
               6:factor_generator(df,[month_for_analysis,month_for_analysis],6),
              }
    df.loc[df.month_adj==month_for_analysis,"projectedfactor_monthlyv1"] = df.loc[df.month_adj==month_for_analysis,"projectedfactor_monthlyv1"].replace(monthly_factor_dict)
df["projected_vol"] = df.volume*df.projectedfactor_monthlyv1/1000000
df["projected_binvalue"] = df.binvalue*df.projectedfactor_monthlyv1
df["projected_pack"] = df.quantity_pack*df.projectedfactor_monthlyv1


# In[25]:


# df.tghh.unique()
df.dtypes


# In[28]:


template_cat = pd.read_csv(r"C:\Users\shivs\Downloads\sscat_bintix.csv")[["cat"]].drop_duplicates().to_dict("records")
template_scat = pd.read_csv(r"C:\Users\shivs\Downloads\sscat_bintix.csv")[["cat","scat"]].drop_duplicates().to_dict("records")
template_sscat = pd.read_csv(r"C:\Users\shivs\Downloads\sscat_bintix.csv")[["cat","scat","sscat"]].drop_duplicates().to_dict("records")
template_custom = [
    {"attribute1":"FMCG"},
    {"attribute2":"FnB"},
    {"cat":"SNACKS & BRANDED FOODS"},
    {"scat":"READY TO EAT CEREALS"},
    {"sscat":"FAMILY FLAKES"},
    {"sscat":"KIDS FLAKES"},
    {"sscat":"MUESLI & GRANOLA"},
    {"sscat":"OATS & PORRIDGE"},
    {"brand":"KELLOGG'S","sub_brand":"CORN FLAKES"},
    {"brand":"KELLOGG'S","sub_brand":"CHOCOS"},
    {"brand":"KELLOGG'S","sscat":"MUESLI & GRANOLA"},
    {"brand":"KELLOGG'S","sub_brand":"OATS"},
    {"choc_type":"PREMIUM CHOCOLATE"},
    {"choc_type":"NON PREMIUM CHOCOLATE"}
]
df.loc[df.cat.isin([list(x.values())[0] for x in template_cat]),"attribute1"] = "FMCG"
df.loc[df.cat.isin(['BAKERY, CAKES & DAIRY','BEVERAGES','FOODGRAINS, OIL & MASALA','SNACKS & BRANDED FOODS']),"attribute2"] = "FnB"
df.loc[(df.sscat=="CHOCOLATES")&(df.std_unit_value.notnull())&((df.price/df.std_unit_value)>=0.83),"choc_type"] = "PREMIUM CHOCOLATE"
df.loc[(df.sscat=="CHOCOLATES")&(df.std_unit_value.notnull())&((df.price/df.std_unit_value)<0.83),"choc_type"] = "NON PREMIUM CHOCOLATE"


# In[29]:


def list_of_month(time_period):
    return list(range(time_period[0],time_period[1]+1))
# list_of_months([2,10])
def average_tghh_calculator(data,time_period,city_list): #for a city or group of cities
    return data.loc[(data.month_adj.isin(list_of_month(time_period))) & (data.city_id.isin(city_list))].groupby(["city_id","month_adj"]).tghh.first().reset_index().groupby('city_id').tghh.mean().sum()
# average_tghh_calculator(df,[28,30],[1,2,3,4,5,6])
def unique_bintix_uid_count(data,time_period,city_list):
    return len(data.loc[(data.month_adj.isin(list_of_month(time_period))) & (data.city_id.isin(city_list))].bintix_uid.unique())
# unique_bintix_uid_count(df,[28,30],[1,2,3,4,5,6])  
def factor_generator(data,time_period,city):
    return average_tghh_calculator(data,time_period,[city])/unique_bintix_uid_count(data,time_period,[city])  
# factor_generator(df,[28,30],1) 
def time_period_number_list_to_name(time_period):
    month_num_dict = {0:"JAN",1:"FEB",2:"MAR",3:"APR",4:"MAY",5:"JUN",6:"JUL",7:"AUG",8:"SEP",9:"OCT",10:"NOV",11:"DEC"}
    tp_start_string = month_num_dict[(time_period[0]-1)%12]+str(19+(time_period[0]-1)//12)
    if time_period[0]==time_period[1]:
        return tp_start_string
    tp_end_string = month_num_dict[(time_period[1]-1)%12]+str(19+(time_period[1]-1)//12)
    return tp_start_string + "-" + tp_end_string

def standard_metrics_new(name,data,template,metric_list,time_period_list,city_list,cat_dict=False):
    data = data.loc[data.city_id.isin(city_list)]
    formula_dict = {
        "HHs": "num_hh",
        "Penetration %": "num_hh/average_tghh",
        "Trial Rate": "num_hh/cat_hh",
        "Volume": "temp.projected_vol.sum()",
        "Value":  "temp.projected_binvalue.sum()",
        "NOP":  "temp.projected_pack.sum()",
        "Avg. Volume": "1000000*temp.projected_vol.sum()/num_hh",
        "Avg. Value": "temp.projected_binvalue.sum()/num_hh",
        "Avg. NOP": "temp.projected_pack.sum()/num_hh",
        "Volume Share": "temp.projected_vol.sum()/cat_projected_vol",
        "Value Share": "temp.projected_binvalue.sum()/cat_projected_binvalue",
        "NOP Share": "temp.projected_pack.sum()/cat_projected_pack",
        "Avg. Pack Size": "1000000*temp.projected_vol.sum()/temp.projected_pack.sum()"
    }
    
    row_names = [" ".join(x) for x in [y.values() for y in template]]
    tp_name_list = [time_period_number_list_to_name(tp) for tp in time_period_list]
#     column_names = pd.MultiIndex.from_product([metric_list,tp_name_list]) #for multi index
    column_name_product = list(product(metric_list, tp_name_list))
    column_names = [" ".join(x) for x in column_name_product]
    
    result_df = pd.DataFrame(index = row_names,columns = column_names)
    num_rows = len(row_names)
    for tp in time_period_list:
        tp_name = time_period_number_list_to_name(tp)
        temp1 = data.loc[data.month_adj.isin(list_of_month(tp))]
        average_tghh = average_tghh_calculator(temp1,tp,city_list)
            
        factor_dict = dict()
        for ci in city_list:
            factor_dict[ci] = factor_generator(temp1,tp,ci)
        temp1["factor"] = temp1.city_id
        temp1["factor"].replace(factor_dict,inplace=True)
        if cat_dict:
            cat_data = temp1.copy()
            for ind in cat_dict:
                cat_data=cat_data.loc[cat_data[ind]==cat_dict[ind]]
            cat_projected_vol = cat_data.projected_vol.sum()
            cat_projected_binvalue = cat_data.projected_binvalue.sum()
            cat_projected_pack = cat_data.projected_pack.sum()
            cat_hh = cat_data.groupby("bintix_uid").factor.first().sum()
        for i in range(num_rows):
            temp = temp1.copy()
            row_dict = template[i]
            for ind in row_dict:
                temp=temp.loc[temp[ind]==row_dict[ind]]
            num_hh = temp.groupby("bintix_uid").factor.first().sum()
            for m in metric_list:
#                 result_df[m,tp_name].iloc[i]= eval(formula_dict[m]) # for multi index
                result_df[m +" "+tp_name].iloc[i]= eval(formula_dict[m])
    result_df.to_csv(name+".csv")
    return result_df.reset_index()

def growth_df_generator(metric_df): #assumes index is reset in metric df and 3 metrics are used (pen,vol,val), hence /3
    growth_df = metric_df.set_index("index").pct_change(axis=1)
    growth_df.iloc[:,0] = np.nan
    growth_df.iloc[:,int(growth_df.shape[1]/3)] = np.nan
    growth_df.iloc[:,int(growth_df.shape[1]/3)*2] = np.nan
    return growth_df.reset_index()

def column_number_to_name(col_num): #Recursion :D
    if col_num==0:
        return ""
    return column_number_to_name((col_num - 1 )//26) + chr((col_num - 1)%26 + 65)

def clean_quarters(start_random,end_random):
    if start_random%3==1:
        start = start_random
    else:
        start = ((start_random-1)//3)*3 +4
    
    end = ((end_random)//3)*3 
    
    clean_quarter_list = []
    for i in range(start,end,3):
        clean_quarter_list+= [[i,i+2]]
    
    return clean_quarter_list
    

def clean_years(start_random,end_random):
    if start_random%12==1:
        start = start_random
    else:
        start = ((start_random-1)//12)*12 +13
    
    end = ((end_random)//12)*12 
    
    clean_year_list = []
    for i in range(start,end,12):
        clean_year_list+= [[i,i+11]]
    
    return clean_year_list

def style_metric(work_sheet, row_num,col_num):
    work_sheet.adjust_column_width(start=1, end=1, pixel_size=300)
    work_sheet.adjust_column_width(start=2, end=col_num, pixel_size=60)
    work_sheet.adjust_row_height(1,1, pixel_size=50) # Updates row height to 50 pixel
    work_sheet.adjust_row_height(2,row_num+1, pixel_size=25) # Updates row height to 50 pixel
    
    
    rng = work_sheet.get_values('B2',column_number_to_name(int((col_num-1)/3)+1) + str(row_num+1) , returnas='range')
    model_cell = pygsheets.Cell("A1")
    model_cell.format = (pygsheets.FormatType.PERCENT, '0.0%')
    model_cell.set_horizontal_alignment( pygsheets.custom_types.HorizontalAlignment.CENTER )
    model_cell.set_vertical_alignment( pygsheets.custom_types.VerticalAlignment.BOTTOM )
    model_cell.set_text_format("fontSize", 6)
    rng.apply_format(model_cell)
    
    rng = work_sheet.get_values(column_number_to_name(int((col_num-1)/3)+2) + "2",column_number_to_name(col_num) + str(row_num+1) , returnas='range')
    model_cell = pygsheets.Cell("A1")
    model_cell.format = (pygsheets.FormatType.NUMBER, '0')
    model_cell.set_horizontal_alignment( pygsheets.custom_types.HorizontalAlignment.CENTER )
    model_cell.set_vertical_alignment( pygsheets.custom_types.VerticalAlignment.BOTTOM )
    model_cell.set_text_format("fontSize", 6)
    rng.apply_format(model_cell)
    
    rng = work_sheet.get_values('A1',column_number_to_name(col_num)+'1', returnas='range')
    model_cell = pygsheets.Cell("A1")
#     model_cell.color = (1.0,0,1.0,1.0) # rose color cell
    model_cell.wrap_strategy = 'WRAP'
    model_cell.set_text_format("bold", True)
    model_cell.set_text_format("fontSize", 6)
    rng.apply_format(model_cell)
    
    rng = work_sheet.get_values('A2',"A"+str(row_num+1) , returnas='range')
    model_cell  = pygsheets.Cell("A1")
    model_cell.set_text_format("fontSize", 6)
    rng.apply_format(model_cell)
    
    
    work_sheet.frozen_cols = 1
    work_sheet.frozen_rows =1

def style_growth(work_sheet, row_num,col_num):
    work_sheet.adjust_column_width(start=1, end=1, pixel_size=300)
    work_sheet.adjust_column_width(start=2, end=col_num, pixel_size=60)
    work_sheet.adjust_row_height(1,1, pixel_size=50) # Updates row height to 50 pixel
    work_sheet.adjust_row_height(2,row_num+1, pixel_size=25) # Updates row height to 50 pixel
    
    rng = work_sheet.get_values('B2',column_number_to_name(col_num) + str(row_num+1) , returnas='range')
    model_cell = pygsheets.Cell("A1")
    model_cell.format = (pygsheets.FormatType.PERCENT, '0.0%')
    model_cell.set_horizontal_alignment( pygsheets.custom_types.HorizontalAlignment.CENTER )
    model_cell.set_vertical_alignment( pygsheets.custom_types.VerticalAlignment.BOTTOM )
    model_cell.set_text_format("fontSize", 6)
    rng.apply_format(model_cell)
    
    rng = work_sheet.get_values('A1',column_number_to_name(col_num)+'1', returnas='range')
    model_cell  = pygsheets.Cell("A1")
#     model_cell.color = (1.0,0,1.0,1.0) # rose color cell
    model_cell.wrap_strategy = 'WRAP'
    model_cell.set_text_format("bold", True)
    model_cell.set_text_format("fontSize", 6)
    rng.apply_format(model_cell)
    
    rng = work_sheet.get_values('A2',"A"+str(row_num+1) , returnas='range')
    model_cell  = pygsheets.Cell("A1")
    model_cell.set_text_format("fontSize", 6)
    rng.apply_format(model_cell)
    
    work_sheet.frozen_cols = 1
    work_sheet.frozen_rows =1
    
    work_sheet.add_conditional_formatting(column_number_to_name(int((col_num-1)/3)+1) + "2", column_number_to_name(int((col_num-1)/3)+1) + str(row_num+1), 'NUMBER_LESS', {'backgroundColor':{'red':1,'green':0.68,'blue':0.73,'alpha':1}}, ["0"])
    work_sheet.add_conditional_formatting(column_number_to_name(int((col_num-1)/3)+1) + "2", column_number_to_name(int((col_num-1)/3)+1) + str(row_num+1), 'NUMBER_GREATER', {'backgroundColor':{'red':.70,'green':.97,'blue':.78,'alpha':1}}, ["0"])
    work_sheet.add_conditional_formatting(column_number_to_name(int((col_num-1)/3)*2+1) + "2", column_number_to_name(int((col_num-1)/3)*2+1) + str(row_num+1), 'NUMBER_LESS', {'backgroundColor':{'red':1,'green':0.68,'blue':0.73,'alpha':1}}, ["0"])
    work_sheet.add_conditional_formatting(column_number_to_name(int((col_num-1)/3)*2+1) + "2", column_number_to_name(int((col_num-1)/3)*2+1) + str(row_num+1), 'NUMBER_GREATER', {'backgroundColor':{'red':.70,'green':.97,'blue':.78,'alpha':1}}, ["0"])
    work_sheet.add_conditional_formatting(column_number_to_name(int((col_num-1)/3)*3+1) + "2", column_number_to_name(int((col_num-1)/3)*3+1) + str(row_num+1), 'NUMBER_LESS', {'backgroundColor':{'red':1,'green':0.68,'blue':0.73,'alpha':1}}, ["0"])
    work_sheet.add_conditional_formatting(column_number_to_name(int((col_num-1)/3)*3+1) + "2", column_number_to_name(int((col_num-1)/3)*3+1) + str(row_num+1), 'NUMBER_GREATER', {'backgroundColor':{'red':.70,'green':.97,'blue':.78,'alpha':1}}, ["0"])


# In[387]:


# result_df = standard_metrics_new("ppt_add",df,template,["Volume",],[[39,40,41]],[1,2,3,4,5,6])
# clean_years(1,27)
# list(range(7,18,3))


# In[30]:


#MONTHLY
city_list_of_list = [[1,2,3,4,5,6],[1],[2],[3],[4],[5],[6]]  

list_of_sheet_link = [
                      'https://docs.google.com/spreadsheets/d/1rBABOu961P51NnzgVYmBbq7tT4iLelxiBpGTkU1RKcE',
                      'https://docs.google.com/spreadsheets/d/1PeNy10DESxfrdIKSWkaKxdtbDGYmGxDktJHBTnyNrfU',
                      'https://docs.google.com/spreadsheets/d/1Ly8aLckHJukLKieOl0dH_HHjOKJEiw9FK0TTYJE0d7A',
                      'https://docs.google.com/spreadsheets/d/1R0S9Cb83MME42FNqfNmUZ4tUCd75w_BVr-3KQWfG6kQ',
                      'https://docs.google.com/spreadsheets/d/1o1DJbTXcGre-lPmVdTmsYzKo6s6co4T6xp91gL2f5nI',
                      'https://docs.google.com/spreadsheets/d/1AM49HmXV3RwZfPv6xkNQXrXMmmmDGv3WOoPSZRsdKkc',
                      'https://docs.google.com/spreadsheets/d/1GXbkNXhYI6vqq8vWE4kGjEQzdmR01pzzd8tNcsaGb3s'
                     ]

for i in range(len(list_of_sheet_link)):
    city_list = city_list_of_list[i]
    start_month = df.loc[df.city_id.isin(city_list)].groupby("city_id").month_adj.min().max()
    end_month = df.loc[df.city_id.isin(city_list)].groupby("city_id").month_adj.max().min()
    list_of_tp_for_analysis = [[x,x] for x in range(start_month,end_month+1)]
    
    gc = pygsheets.authorize(service_file=r'C:\Users\shivs\OneDrive\Documents\pygsheetstest-bintix-6d60b820241d.json')

    #open the google spreadsheet 
    sh = gc.open_by_url(list_of_sheet_link[i])

    #select the first sheet 
#     wks_cat = sh[0]
#     wks_scat = sh[1]
#     wks_sscat = sh[2]
#     wks_cat_growth = sh[3]
#     wks_scat_growth = sh[4]
#     wks_sscat_growth = sh[5]
#     wks_custom = sh[6]
#     wks_custom_growth = sh[7]

    sh.del_worksheet(sh.worksheet_by_title("CAT"))
    sh.add_worksheet("CAT")
    wks_cat = sh.worksheet_by_title("CAT")
    
    sh.del_worksheet(sh.worksheet_by_title("SCAT"))
    sh.add_worksheet("SCAT")
    wks_scat = sh.worksheet_by_title("SCAT")
    
    sh.del_worksheet(sh.worksheet_by_title("SSCAT"))
    sh.add_worksheet("SSCAT")
    wks_sscat = sh.worksheet_by_title("SSCAT")
    
    sh.del_worksheet(sh.worksheet_by_title("CAT_growth_MoM"))
    sh.add_worksheet("CAT_growth_MoM")
    wks_cat_growth = sh.worksheet_by_title("CAT_growth_MoM")
    
    sh.del_worksheet(sh.worksheet_by_title("SCAT_growth_MoM"))
    sh.add_worksheet("SCAT_growth_MoM")
    wks_scat_growth = sh.worksheet_by_title("SCAT_growth_MoM")
    
    sh.del_worksheet(sh.worksheet_by_title("SSCAT_growth_MoM"))
    sh.add_worksheet("SSCAT_growth_MoM")
    wks_sscat_growth = sh.worksheet_by_title("SSCAT_growth_MoM")
    
    sh.del_worksheet(sh.worksheet_by_title("Custom"))
    sh.add_worksheet("Custom")
    wks_custom = sh.worksheet_by_title("Custom")
    
    sh.del_worksheet(sh.worksheet_by_title("Custom_growth_MoM"))
    sh.add_worksheet("Custom_growth_MoM")
    wks_custom_growth = sh.worksheet_by_title("Custom_growth_MoM")
    
    cat_df = standard_metrics_new("cat",df,template_cat,["Penetration %","Volume","Value"],list_of_tp_for_analysis,city_list)
    scat_df = standard_metrics_new("scat",df,template_scat,["Penetration %","Volume","Value"],list_of_tp_for_analysis,city_list)
    sscat_df = standard_metrics_new("sscat",df,template_sscat,["Penetration %","Volume","Value"],list_of_tp_for_analysis,city_list)
    custom_df = standard_metrics_new("custom",df,template_custom,["Penetration %","Volume","Value"],list_of_tp_for_analysis,city_list)
 
    wks_cat.set_dataframe(cat_df, 'A1')
    wks_scat.set_dataframe(scat_df, 'A1')
    wks_sscat.set_dataframe(sscat_df, 'A1')
    wks_custom.set_dataframe(custom_df, 'A1')
    wks_cat_growth.set_dataframe(growth_df_generator(cat_df), 'A1')
    wks_scat_growth.set_dataframe(growth_df_generator(scat_df), 'A1')
    wks_sscat_growth.set_dataframe(growth_df_generator(sscat_df), 'A1')
    wks_custom_growth.set_dataframe(growth_df_generator(custom_df), 'A1')
    
    style_metric(wks_cat,cat_df.shape[0],cat_df.shape[1])
    style_metric(wks_scat,scat_df.shape[0],scat_df.shape[1])
    style_metric(wks_sscat,sscat_df.shape[0],sscat_df.shape[1])
    style_metric(wks_custom,custom_df.shape[0],custom_df.shape[1])
    style_growth(wks_cat_growth,cat_df.shape[0],cat_df.shape[1])
    style_growth(wks_scat_growth,scat_df.shape[0],scat_df.shape[1])
    style_growth(wks_sscat_growth,sscat_df.shape[0],sscat_df.shape[1])
    style_growth(wks_custom_growth,custom_df.shape[0],custom_df.shape[1])
    
    print(i)

# spreadsheet.share('example@gmail.com', role='commenter', type='user', emailMessage='Here is the spreadsheet we talked about!')
#MONTHLY


# In[15]:


#QUARTERLY
city_list_of_list = [[1,2,3,4,5,6],[1],[2],[3],[4],[5],[6]] 

list_of_sheet_link = [
                      'https://docs.google.com/spreadsheets/d/1g7TJkwrwXAYVTDgeFncjSlfM1UrryrnLP2mZEYaFrfY',
                      'https://docs.google.com/spreadsheets/d/1Zwyo5Dg3ceOgmjKNemkWm2FEA_uA0cXH-T7helcmV4Y',
                      'https://docs.google.com/spreadsheets/d/1iiooEHuZjXByz1HgIRd9ddXa-DXuABwe5idV1krg7hc',
                      'https://docs.google.com/spreadsheets/d/1cjUU6whOlGLKxdkxYnOeGagbF-saDNz5iz3Ug9lgHuc',
                      'https://docs.google.com/spreadsheets/d/1D6wo8xt9qcU3RDXsq0RHEVt3hoeX04NUd16ntZInbHk',
                      'https://docs.google.com/spreadsheets/d/1sLSaysGjtd23celetrKBc_XNVcm6zOxTzRIyut8tXLQ',
                      'https://docs.google.com/spreadsheets/d/1Gcn7IXdBR-9s4yYnV2I5Hr-PDVBcrDtg_EgnSwWG8mE'
                     ]

for i in range(len(list_of_sheet_link)):
    city_list = city_list_of_list[i]
    start_month = df.loc[df.city_id.isin(city_list)].groupby("city_id").month_adj.min().max()
    end_month = df.loc[df.city_id.isin(city_list)].groupby("city_id").month_adj.max().min()
    list_of_tp_for_analysis = clean_quarters(start_month,end_month)
    
    gc = pygsheets.authorize(service_file=r'C:\Users\shivs\OneDrive\Documents\pygsheetstest-bintix-6d60b820241d.json')

    #open the google spreadsheet 
    sh = gc.open_by_url(list_of_sheet_link[i])

    #select the first sheet 
#     wks_cat = sh[0]
#     wks_scat = sh[1]
#     wks_sscat = sh[2]
#     wks_cat_growth = sh[3]
#     wks_scat_growth = sh[4]
#     wks_sscat_growth = sh[5]
#     wks_custom = sh[6]
#     wks_custom_growth = sh[7]

    sh.del_worksheet(sh.worksheet_by_title("CAT"))
    sh.add_worksheet("CAT")
    wks_cat = sh.worksheet_by_title("CAT")
    
    sh.del_worksheet(sh.worksheet_by_title("SCAT"))
    sh.add_worksheet("SCAT")
    wks_scat = sh.worksheet_by_title("SCAT")
    
    sh.del_worksheet(sh.worksheet_by_title("SSCAT"))
    sh.add_worksheet("SSCAT")
    wks_sscat = sh.worksheet_by_title("SSCAT")
    
    sh.del_worksheet(sh.worksheet_by_title("CAT_growth_QoQ"))
    sh.add_worksheet("CAT_growth_QoQ")
    wks_cat_growth = sh.worksheet_by_title("CAT_growth_QoQ")
    
    sh.del_worksheet(sh.worksheet_by_title("SCAT_growth_QoQ"))
    sh.add_worksheet("SCAT_growth_QoQ")
    wks_scat_growth = sh.worksheet_by_title("SCAT_growth_QoQ")
    
    sh.del_worksheet(sh.worksheet_by_title("SSCAT_growth_QoQ"))
    sh.add_worksheet("SSCAT_growth_QoQ")
    wks_sscat_growth = sh.worksheet_by_title("SSCAT_growth_QoQ")
    
    sh.del_worksheet(sh.worksheet_by_title("Custom"))
    sh.add_worksheet("Custom")
    wks_custom = sh.worksheet_by_title("Custom")
    
    sh.del_worksheet(sh.worksheet_by_title("Custom_growth_QoQ"))
    sh.add_worksheet("Custom_growth_QoQ")
    wks_custom_growth = sh.worksheet_by_title("Custom_growth_QoQ")
    
    cat_df = standard_metrics_new("cat",df,template_cat,["Penetration %","Volume","Value"],list_of_tp_for_analysis,city_list)
    scat_df = standard_metrics_new("scat",df,template_scat,["Penetration %","Volume","Value"],list_of_tp_for_analysis,city_list)
    sscat_df = standard_metrics_new("sscat",df,template_sscat,["Penetration %","Volume","Value"],list_of_tp_for_analysis,city_list)
    custom_df = standard_metrics_new("custom",df,template_custom,["Penetration %","Volume","Value"],list_of_tp_for_analysis,city_list)
    #update the first sheet with df, starting at cell B2. 
    wks_cat.set_dataframe(cat_df, 'A1')
    wks_scat.set_dataframe(scat_df, 'A1')
    wks_sscat.set_dataframe(sscat_df, 'A1')
    wks_custom.set_dataframe(custom_df, 'A1')
    wks_cat_growth.set_dataframe(growth_df_generator(cat_df), 'A1')
    wks_scat_growth.set_dataframe(growth_df_generator(scat_df), 'A1')
    wks_sscat_growth.set_dataframe(growth_df_generator(sscat_df), 'A1')
    wks_custom_growth.set_dataframe(growth_df_generator(custom_df), 'A1')
    
    style_metric(wks_cat,cat_df.shape[0],cat_df.shape[1])
    style_metric(wks_scat,scat_df.shape[0],scat_df.shape[1])
    style_metric(wks_sscat,sscat_df.shape[0],sscat_df.shape[1])
    style_metric(wks_custom,custom_df.shape[0],custom_df.shape[1])
    style_growth(wks_cat_growth,cat_df.shape[0],cat_df.shape[1])
    style_growth(wks_scat_growth,scat_df.shape[0],scat_df.shape[1])
    style_growth(wks_sscat_growth,sscat_df.shape[0],sscat_df.shape[1])
    style_growth(wks_custom_growth,custom_df.shape[0],custom_df.shape[1])
    
    print(i)

# spreadsheet.share('example@gmail.com', role='commenter', type='user', emailMessage='Here is the spreadsheet we talked about!')
#QUARTERLY


# In[398]:


#YEARLY
city_list_of_list = [[1,2,3,4,5,6],[1],[2],[3],[4],[5],[6]] 

list_of_sheet_link = [
                      'https://docs.google.com/spreadsheets/d/19x7KyJCA_QhTeaH3PzW3lSdLvq_CO493zpsiCYSBKn8',
                      'https://docs.google.com/spreadsheets/d/1DEyPseSQK12LJH-cYWoS5TJ3zGdnFC_cljD4SRROrWk',
                      'https://docs.google.com/spreadsheets/d/15jhlYjyi3q4uX3SI5WKJ9LVD660jXISRxPRD-U16x10',
                      'https://docs.google.com/spreadsheets/d/1iWIb1R-1BsFUey9L7qqyCGhK-KIwlaJ35sUNn0AZAT4',
                      'https://docs.google.com/spreadsheets/d/1eStLY4l-QOwwluRllrTF4BrRXlWa5-m-dUtLdvUyX-A',
                      'https://docs.google.com/spreadsheets/d/12YhrusypPiYfZ2S2TCJm4buRLvzVBkFueQGwg2Hx0OM',
                      'https://docs.google.com/spreadsheets/d/1SrNQb2E98ewHcdsvIHpLRvLm_G9YJmF5oe7y4-zaQDQ'
                     ]

for i in range(len(list_of_sheet_link)): 
    city_list = city_list_of_list[i]
    start_month = df.loc[df.city_id.isin(city_list)].groupby("city_id").month_adj.min().max()
    end_month = df.loc[df.city_id.isin(city_list)].groupby("city_id").month_adj.max().min()
    list_of_tp_for_analysis = clean_years(start_month,end_month)
    
    gc = pygsheets.authorize(service_file=r'C:\Users\shivs\OneDrive\Documents\pygsheetstest-bintix-6d60b820241d.json')

    #open the google spreadsheet 
    sh = gc.open_by_url(list_of_sheet_link[i])

    #select the first sheet 
#     wks_cat = sh[0]
#     wks_scat = sh[1]
#     wks_sscat = sh[2]
#     wks_cat_growth = sh[3]
#     wks_scat_growth = sh[4]
#     wks_sscat_growth = sh[5]
#     wks_custom = sh[6]
#     wks_custom_growth = sh[7]

    sh.del_worksheet(sh.worksheet_by_title("CAT"))
    sh.add_worksheet("CAT")
    wks_cat = sh.worksheet_by_title("CAT")
    
    sh.del_worksheet(sh.worksheet_by_title("SCAT"))
    sh.add_worksheet("SCAT")
    wks_scat = sh.worksheet_by_title("SCAT")
    
    sh.del_worksheet(sh.worksheet_by_title("SSCAT"))
    sh.add_worksheet("SSCAT")
    wks_sscat = sh.worksheet_by_title("SSCAT")
    
    sh.del_worksheet(sh.worksheet_by_title("CAT_growth_YoY"))
    sh.add_worksheet("CAT_growth_YoY")
    wks_cat_growth = sh.worksheet_by_title("CAT_growth_YoY")
    
    sh.del_worksheet(sh.worksheet_by_title("SCAT_growth_YoY"))
    sh.add_worksheet("SCAT_growth_YoY")
    wks_scat_growth = sh.worksheet_by_title("SCAT_growth_YoY")
    
    sh.del_worksheet(sh.worksheet_by_title("SSCAT_growth_YoY"))
    sh.add_worksheet("SSCAT_growth_YoY")
    wks_sscat_growth = sh.worksheet_by_title("SSCAT_growth_YoY")
    
    sh.del_worksheet(sh.worksheet_by_title("Custom"))
    sh.add_worksheet("Custom")
    wks_custom = sh.worksheet_by_title("Custom")
    
    sh.del_worksheet(sh.worksheet_by_title("Custom_growth_YoY"))
    sh.add_worksheet("Custom_growth_YoY")
    wks_custom_growth = sh.worksheet_by_title("Custom_growth_YoY")
    
    cat_df = standard_metrics_new("cat",df,template_cat,["Penetration %","Volume","Value"],list_of_tp_for_analysis,city_list)
    scat_df = standard_metrics_new("scat",df,template_scat,["Penetration %","Volume","Value"],list_of_tp_for_analysis,city_list)
    sscat_df = standard_metrics_new("sscat",df,template_sscat,["Penetration %","Volume","Value"],list_of_tp_for_analysis,city_list)
    custom_df = standard_metrics_new("custom",df,template_custom,["Penetration %","Volume","Value"],list_of_tp_for_analysis,city_list)
    #update the first sheet with df, starting at cell B2. 
    wks_cat.set_dataframe(cat_df, 'A1')
    wks_scat.set_dataframe(scat_df, 'A1')
    wks_sscat.set_dataframe(sscat_df, 'A1')
    wks_custom.set_dataframe(custom_df, 'A1')
    wks_cat_growth.set_dataframe(growth_df_generator(cat_df), 'A1')
    wks_scat_growth.set_dataframe(growth_df_generator(scat_df), 'A1')
    wks_sscat_growth.set_dataframe(growth_df_generator(sscat_df), 'A1')
    wks_custom_growth.set_dataframe(growth_df_generator(custom_df), 'A1')
    
    style_metric(wks_cat,cat_df.shape[0],cat_df.shape[1])
    style_metric(wks_scat,scat_df.shape[0],scat_df.shape[1])
    style_metric(wks_sscat,sscat_df.shape[0],sscat_df.shape[1])
    style_metric(wks_custom,custom_df.shape[0],custom_df.shape[1])
    style_growth(wks_cat_growth,cat_df.shape[0],cat_df.shape[1])
    style_growth(wks_scat_growth,scat_df.shape[0],scat_df.shape[1])
    style_growth(wks_sscat_growth,sscat_df.shape[0],sscat_df.shape[1])
    style_growth(wks_custom_growth,custom_df.shape[0],custom_df.shape[1])
    
    print(i)

# spreadsheet.share('example@gmail.com', role='commenter', type='user', emailMessage='Here is the spreadsheet we talked about!')
#YEARLY


# # END. ROUGH WORK BELOW.
