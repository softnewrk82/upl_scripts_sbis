import requests
import json

import cryptography

import pandas as pd
import numpy as np

import xmltodict

import re
import requests

import warnings
warnings.simplefilter("ignore")

from functools import lru_cache

import importlib

import modules.api_info
importlib.reload(modules.api_info)

from datetime import datetime

from sqlalchemy import create_engine

from modules.api_info import var_encrypt_var_app_client_id
from modules.api_info import var_encrypt_var_app_secret
from modules.api_info import var_encrypt_var_secret_key

from modules.api_info import var_encrypt_url_sbis
from modules.api_info import var_encrypt_url_sbis_unloading

from modules.api_info import var_encrypt_var_db_user_name
from modules.api_info import var_encrypt_var_db_user_pass

from modules.api_info import var_encrypt_var_db_host
from modules.api_info import var_encrypt_var_db_port

from modules.api_info import var_encrypt_var_db_name
from modules.api_info import var_encrypt_var_db_name_for_upl
from modules.api_info import var_encrypt_var_db_schema
from modules.api_info import var_encryptvar_API_sbis
from modules.api_info import var_encrypt_API_sbis_pass

from modules.api_info import f_decrypt, load_key_external


var_app_client_id = f_decrypt(var_encrypt_var_app_client_id, load_key_external()).decode("utf-8")
var_app_secret = f_decrypt(var_encrypt_var_app_secret, load_key_external()).decode("utf-8")
var_secret_key = f_decrypt(var_encrypt_var_secret_key, load_key_external()).decode("utf-8")

url_sbis = f_decrypt(var_encrypt_url_sbis, load_key_external()).decode("utf-8")
url_sbis_unloading = f_decrypt(var_encrypt_url_sbis_unloading, load_key_external()).decode("utf-8")

var_db_user_name = f_decrypt(var_encrypt_var_db_user_name, load_key_external()).decode("utf-8")
var_db_user_pass = f_decrypt(var_encrypt_var_db_user_pass, load_key_external()).decode("utf-8")

var_db_host = f_decrypt(var_encrypt_var_db_host, load_key_external()).decode("utf-8")
var_db_port = f_decrypt(var_encrypt_var_db_port, load_key_external()).decode("utf-8")

var_db_name = f_decrypt(var_encrypt_var_db_name, load_key_external()).decode("utf-8")

var_db_name_for_upl = f_decrypt(var_encrypt_var_db_name_for_upl, load_key_external()).decode("utf-8")


var_db_schema = f_decrypt(var_encrypt_var_db_schema, load_key_external()).decode("utf-8")

API_sbis = f_decrypt(var_encryptvar_API_sbis, load_key_external()).decode("utf-8")
API_sbis_pass = f_decrypt(var_encrypt_API_sbis_pass, load_key_external()).decode("utf-8")



from modules.api_info import var_encrypt_TOKEN_yandex_users, f_decrypt, load_key_external
from modules.api_info import var_encrypt_var_login_da, var_encrypt_var_pass_da
# ____________________________________________________________________________________________

var_TOKEN = f_decrypt(var_encrypt_TOKEN_yandex_users, load_key_external()).decode("utf-8")
login_da = f_decrypt(var_encrypt_var_login_da, load_key_external()).decode("utf-8")
pass_da = f_decrypt(var_encrypt_var_pass_da, load_key_external()).decode("utf-8")


# var_day = '01'
# var_month = '04'
# var_year = '2024'
# 
# date_from = "21.06.2024"
# date_to = "21.06.2024"

# var_day = ''
# var_month = ''
# var_year = ''

# date_from = ""
# date_to = ""



def send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn):
        
        var_login_da = str(login_da)
        var_pass_da = str(pass_da)


        # myuuid_sbis_down = str(uuid.uuid4())
        myuuid_sbis_down = "b57d09dc-a631-4dbc-9e01-f706920fcb29"
        # print('Your UUID is: ' + str(myuuid_sbis_down))
        # _________________________________

        url = "https://online.sbis.ru/auth/service/" 


        method = "СБИС.Аутентифицировать"

        params = {
            "Параметр": {
                "Логин": f"{var_login_da}",
                "Пароль": f"{var_pass_da}",
            }

        }
        parameters = {
        "jsonrpc": "2.0",
        "method": method,
        "params":params,
        "id": 0
        }
            
        response = requests.post(url, json=parameters)
        response.encoding = 'utf-8'


        str_to_dict = json.loads(response.text)
        access_token = str_to_dict["result"]
        # print("access_token:", access_token)

        headers = {
        "X-SBISSessionID": access_token,
        "Content-Type": "application/json",
        }  

        # _____________________________________________________________


        parameters_real = {

        "jsonrpc": "2.0",
        "protocol": 6,
        "method": "PublicMsgApi.MessageSend",
        "params": {
            "dialogID": myuuid_sbis_down,
            "messageID": None,
            "answer": None,
            "text": f"{datetime.now().date()} {var_link}, {var_doc_number}, {var_doc_data_main}, {var_doc_type}, {var_doc_counterparty_inn}",
            "document": "bc70081f-3ccf-4507-b753-0e185191bf8c",
            "files": {
                "fileId": "bc70081f-3ccf-4507-b753-0e185191bf8c",
                "isLink": "true",
            },
            "recipients": [
            "c943a420-f494-4a38-8975-d9db61c3dba7",
            # "dc283f5c-05fc-11ee-812e-3c846acc6838",
            ],
            "options": {
                "d": [
                    "СБИС.API (errors)",
                    0,
                    {}
                ],
                "s": [
                    {
                    "t": "Строка",
                    "n": "Title"
                    },
                    {
                    "t": "Число целое",
                    "n": "TextFormat"
                    },
                    {
                    "t": "JSON-объект",
                    "n": "ServiceObject"
                    }
                ],
                "_type": "record"
                }
            },
            "id": 1
            }



        url_real = "https://online.sbis.ru/msg/service/"

        response_points = requests.post(url_real, json=parameters_real, headers=headers)
        # str_to_dict_points = json.loads(response_points.text)
        # str_to_dict_points["result"]["d"][3]["chat_name"]


# _____________________________________________________________________________
# _____________________________________________________________________________
# _____________________________________________________________________________

datetime_from_returnin = datetime.now().date() - relativedelta(years=2)
datetime_to_returnin = datetime.now().date()

# ________________________________________________________
if len(str(datetime_from_returnin.month)) == 1:
    var_month_from = '0' + f'{datetime_from_returnin.month}'
else:
    var_month_from = f'{datetime_from_returnin.month}'

if len(str(datetime_from_returnin.day)) == 1:
    var_day_from = '0' + f'{datetime_from_returnin.day}'
else:
    var_day_from = f'{datetime_from_returnin.day}'
# ________________________________________________________


# ________________________________________________________
if len(str(datetime_to_returnin.month)) == 1:
    var_month_to = '0' + f'{datetime_to_returnin.month}'
else:
    var_month_to = f'{datetime_to_returnin.month}'

if len(str(datetime_to_returnin.day)) == 1:
    var_day_to = '0' + f'{datetime_to_returnin.day}'
else:
    var_day_to = f'{datetime_to_returnin.day}'
# ________________________________________________________


date_from_returnin = f'{var_day_from}' + '.' + f'{var_month_from}' + '.' + f'{datetime_from_returnin.year}'
date_to_returnin = f'{var_day_to}' + '.' + f'{var_month_to}' + '.' + f'{datetime_to_returnin.year}'
name_unloading = 'returnin_upl_sbis'

print('date_from_returnin:', date_from_returnin)
print('date_to_returnin:', date_to_returnin)
print('returnin_upl_sbis:', name_unloading)

# ________________________________________________________
# ________________________________________________________
# ________________________________________________________


def sbis_real_processing_returnin(date_from_returnin, date_to_returnin, name_unloading):
    
    date_from = date_from_returnin
    date_to = date_to_returnin

    print("date_from:", date_from)
    print("date_to:", date_to)

    def doc_append():
        doc_id.append(var_link)
        doc_type.append(var_doc_type)
        doc_number.append(var_doc_number)
        doc_full_name.append(var_doc_full_name)
        doc_data_main.append(var_doc_data_main)
        doc_at_created.append(var_doc_at_created)
        doc_counterparty_inn.append(var_doc_counterparty_inn)
        doc_counterparty_full_name.append(var_doc_counterparty_full_name)
        doc_provider_inn.append(var_doc_provider_inn)
        doc_provider_full_name.append(var_doc_provider_full_name)

        doc_assigned_manager.append(var_doc_assigned_manager)
        doc_department.append(var_doc_department)

        # print('doc_append')



    def inside_doc_append(var_inside_doc_author,
                        var_inside_doc_type,
                        var_inside_doc_item_full_doc_price_to,
                        var_inside_doc_item_full_doc_price_after,
                        var_inside_doc_item_note,
                        var_inside_doc_item_code,
                        var_inside_doc_item_article,
                        var_inside_doc_item_name,
                        var_inside_doc_item_quantity_to,
                        var_inside_doc_item_quantity_after,
                        var_inside_doc_item_unit_to,
                        var_inside_doc_item_unit_after,
                        var_inside_doc_item_price_to,
                        var_inside_doc_item_price_after,
                        var_inside_doc_item_full_item_price_to,
                        var_inside_doc_item_full_item_price_after,):
        
        inside_doc_author.append(var_inside_doc_author)
        inside_doc_type.append(var_inside_doc_type)
        inside_doc_item_full_doc_price_to.append(var_inside_doc_item_full_doc_price_to)
        inside_doc_item_full_doc_price_after.append(var_inside_doc_item_full_doc_price_after)
        
        inside_doc_item_note.append(var_inside_doc_item_note)
        
        inside_doc_item_code.append(var_inside_doc_item_code)
        inside_doc_item_article.append(var_inside_doc_item_article)
        inside_doc_item_name.append(var_inside_doc_item_name)
        
        inside_doc_item_quantity_to.append(var_inside_doc_item_quantity_to)
        inside_doc_item_quantity_after.append(var_inside_doc_item_quantity_after)
        
        inside_doc_item_unit_to.append(var_inside_doc_item_unit_to)
        inside_doc_item_unit_after.append(var_inside_doc_item_unit_after)
        
        inside_doc_item_price_to.append(var_inside_doc_item_price_to)
        inside_doc_item_price_after.append(var_inside_doc_item_price_after)
        
        inside_doc_item_full_item_price_to.append(var_inside_doc_item_full_item_price_to)
        inside_doc_item_full_item_price_after.append(var_inside_doc_item_full_item_price_after)

        # print('inside_doc_append')
        


    def def_ukd_s_fdis(xml_a, var_inside_doc_author, var_inside_doc_type):
            
            # _________________________________________________________________________________________________________________________________________________            

            def def_ukd_s_fdis_set_variable(var_inside_doc_author, var_inside_doc_type):
                
                # _______________________________________
                # DICT
                # _______________________________________


                if type(xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]) == dict:
                
                    var_inside_doc_item_note = ""
                    
                    sum_inside_doc_to = 0
                    sum_inside_doc_after = 0
                    try:
                        sum_inside_doc_to = float(xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]["СтТовУчНал"]["@СтоимДоИзм"])
                        sum_inside_doc_after = float(xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]["СтТовУчНал"]["@СтоимПослеИзм"])
                    except:
                        sum_inside_doc_to = np.nan
                        sum_inside_doc_after = np.nan
                
                    # print(sum_inside_doc_to, sum_inside_doc_after)
                        
                    var_inside_doc_author = var_inside_doc_author
                    var_inside_doc_type = var_inside_doc_type
                    var_inside_doc_item_full_doc_price_to = sum_inside_doc_to
                    var_inside_doc_item_full_doc_price_after = sum_inside_doc_after
                    
                    var_inside_doc_item_code = xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]["ДопСведТов"]["@КодТов"]
                    
                    
                    try:
                        var_inside_doc_item_article =  xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]["ДопСведТов"]["@АртикулТов"]
                    except: 
                        var_inside_doc_item_article = np.nan
                        
                    var_inside_doc_item_name = xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]["@НаимТов"]
                    
                    try:
                        var_inside_doc_item_quantity_to = xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]["@КолТовДо"]
                        var_inside_doc_item_quantity_after = xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]["@КолТовПосле"]
                    except: 
                        var_inside_doc_item_quantity_to = np.nan
                        var_inside_doc_item_quantity_after = np.nan
                    
                    try:
                        var_inside_doc_item_unit_to = xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]["ДопСведТов"]["@НаимЕдИзмДо"]
                        var_inside_doc_item_unit_after = xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]["ДопСведТов"]["@НаимЕдИзмПосле"]
                    except:
                        var_inside_doc_item_unit_to = np.nan
                        var_inside_doc_item_unit_after = np.nan
                        
                    try:
                        var_inside_doc_item_price_to = xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]["@ЦенаТовДо"]
                        var_inside_doc_item_price_after = xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]["@ЦенаТовПосле"]
                    except: 
                        var_inside_doc_item_price_to = np.nan
                        var_inside_doc_item_price_after = np.nan
                        
                    try:
                        var_inside_doc_item_full_item_price_to = float(xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]["СтТовУчНал"]["@СтоимДоИзм"])
                        var_inside_doc_item_full_item_price_after = float(xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]["СтТовУчНал"]["@СтоимПослеИзм"])
                    except:
                        var_inside_doc_item_full_item_price_to = np.nan
                        var_inside_doc_item_full_item_price_after = np.nan
                
                
                
                    # print("var_inside_doc_item_full_doc_price_to:", var_inside_doc_item_full_doc_price_to)
                    # print("var_inside_doc_item_full_doc_price_after:", var_inside_doc_item_full_doc_price_after)
                    
                    # print("var_inside_doc_item_note:", var_inside_doc_item_note)
                    # print("var_inside_doc_item_code:", var_inside_doc_item_code)
                    # print("var_inside_doc_item_article:", var_inside_doc_item_article)
                    # print("var_inside_doc_item_name:", var_inside_doc_item_name)
                    
                    # print("var_inside_doc_item_quantity_to:", var_inside_doc_item_quantity_to)
                    # print("var_inside_doc_item_quantity_after:", var_inside_doc_item_quantity_after)
                    
                    # print("var_inside_doc_item_unit_after:", var_inside_doc_item_unit_after)
                    # print("var_inside_doc_item_unit_after:", var_inside_doc_item_unit_after)
                    
                    # print("var_inside_doc_item_price_to:", var_inside_doc_item_price_to)
                    # print("var_inside_doc_item_price_after:", var_inside_doc_item_price_after)
                    
                    # print("var_inside_doc_item_full_item_price_to:", var_inside_doc_item_full_item_price_to)
                    # print("var_inside_doc_item_full_item_price_after:", var_inside_doc_item_full_item_price_after)
                

                    doc_append()
                    inside_doc_append(var_inside_doc_author,
                            var_inside_doc_type,
                            var_inside_doc_item_full_doc_price_to,
                            var_inside_doc_item_full_doc_price_after,
                            var_inside_doc_item_note,
                            var_inside_doc_item_code,
                            var_inside_doc_item_article,
                            var_inside_doc_item_name,
                            var_inside_doc_item_quantity_to,
                            var_inside_doc_item_quantity_after,
                            var_inside_doc_item_unit_to,
                            var_inside_doc_item_unit_after,
                            var_inside_doc_item_price_to,
                            var_inside_doc_item_price_after,
                            var_inside_doc_item_full_item_price_to,
                            var_inside_doc_item_full_item_price_after,
                            )

                    # print('_______________________')
                    # print('_______________________')
                    # print('_______________________')
                
                # _______________________________________
                # LIST
                # _______________________________________
                
                elif type(xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]) == list:
                
                
                    for k in xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]:                     
                
                        # print(k)
                                    
                        var_inside_doc_item_note = ""
                        
                        sum_inside_doc_to = 0
                        sum_inside_doc_after = 0
                                    
                        for z in xml_a["Файл"]["Документ"]["ТаблКСчФ"]["СведТов"]:
                            sum_inside_doc_to += float(z["СтТовУчНал"]["@СтоимДоИзм"])
                            sum_inside_doc_after += float(z["СтТовУчНал"]["@СтоимДоИзм"])
                
                
                        var_inside_doc_author = var_inside_doc_author
                        var_inside_doc_type = var_inside_doc_type
                        var_inside_doc_item_full_doc_price_to = sum_inside_doc_to
                        var_inside_doc_item_full_doc_price_after = sum_inside_doc_after
                        
                        var_inside_doc_item_code = k["ДопСведТов"]["@КодТов"]
                        try:
                            var_inside_doc_item_article =  k["ДопСведТов"]["@АртикулТов"]
                        except:
                            var_inside_doc_item_article =  np.nan
                            
                        var_inside_doc_item_name = k["@НаимТов"]
                        
                        try:
                            var_inside_doc_item_quantity_to = k["@КолТовДо"]
                            var_inside_doc_item_quantity_after = k["@КолТовПосле"]
                        except:
                            var_inside_doc_item_quantity_to = np.nan
                            var_inside_doc_item_quantity_after = np.nan
                
                        try:
                            var_inside_doc_item_unit_to = k["ДопСведТов"]["@НаимЕдИзмДо"]
                            var_inside_doc_item_unit_after = k["ДопСведТов"]["@НаимЕдИзмПосле"]
                        except:
                            var_inside_doc_item_unit_to = np.nan
                            var_inside_doc_item_unit_after = np.nan
                        try:
                            var_inside_doc_item_price_to = k["@ЦенаТовДо"]
                            var_inside_doc_item_price_after = k["@ЦенаТовПосле"]
                        except:
                            var_inside_doc_item_price_to = 0
                            var_inside_doc_item_price_after = 0
                        try:
                            var_inside_doc_item_full_item_price_to = float(k["СтТовУчНал"]["@СтоимДоИзм"])
                            var_inside_doc_item_full_item_price_after = float(k["СтТовУчНал"]["@СтоимПослеИзм"])
                        except: 
                            var_inside_doc_item_full_item_price_to = np.nan
                            var_inside_doc_item_full_item_price_after = np.nan
                
                
                        # print("var_inside_doc_item_full_doc_price_to:", var_inside_doc_item_full_doc_price_to)
                        # print("var_inside_doc_item_full_doc_price_after:", var_inside_doc_item_full_doc_price_after)
                        
                        # print("var_inside_doc_item_note:", var_inside_doc_item_note)
                        # print("var_inside_doc_item_code:", var_inside_doc_item_code)
                        # print("var_inside_doc_item_article:", var_inside_doc_item_article)
                        # print("var_inside_doc_item_name:", var_inside_doc_item_name)
                        
                        # print("var_inside_doc_item_quantity_to:", var_inside_doc_item_quantity_to)
                        # print("var_inside_doc_item_quantity_after:", var_inside_doc_item_quantity_after)
                        
                        # print("var_inside_doc_item_unit_after:", var_inside_doc_item_unit_after)
                        # print("var_inside_doc_item_unit_after:", var_inside_doc_item_unit_after)
                        
                        # print("var_inside_doc_item_price_to:", var_inside_doc_item_price_to)
                        # print("var_inside_doc_item_price_after:", var_inside_doc_item_price_after)
                        
                        # print("var_inside_doc_item_full_item_price_to:", var_inside_doc_item_full_item_price_to)
                        # print("var_inside_doc_item_full_item_price_after:", var_inside_doc_item_full_item_price_after)
                
                        # print('_____________________________')
                        # print('_____________________________')
                        # print('_____________________________')
        

        
                        doc_append()
                        inside_doc_append(var_inside_doc_author,
                            var_inside_doc_type,
                            var_inside_doc_item_full_doc_price_to,
                            var_inside_doc_item_full_doc_price_after,
                            var_inside_doc_item_note,
                            var_inside_doc_item_code,
                            var_inside_doc_item_article,
                            var_inside_doc_item_name,
                            var_inside_doc_item_quantity_to,
                            var_inside_doc_item_quantity_after,
                            var_inside_doc_item_unit_to,
                            var_inside_doc_item_unit_after,
                            var_inside_doc_item_price_to,
                            var_inside_doc_item_price_after,
                            var_inside_doc_item_full_item_price_to,
                            var_inside_doc_item_full_item_price_after,
                            )
                        # print('_______________________')
                        # print('_______________________')
                        # print('_______________________')
        

             
                        
        
            # _________________________________________________________________________________________________________________________________________________            

            def_ukd_s_fdis_set_variable(var_inside_doc_author, var_inside_doc_type)
            
            
    url = url_sbis

    method = "СБИС.Аутентифицировать"
    params = {
        "Параметр": {
            "Логин": API_sbis,
            "Пароль": API_sbis_pass
        }

    }
    parameters = {
    "jsonrpc": "2.0",
    "method": method,
    "params": params,
    "id": 0
    }

    response = requests.post(url, json=parameters)
    response.encoding = 'utf-8'

    str_to_dict = json.loads(response.text)
    access_token = str_to_dict["result"]
    # print("access_token:", access_token)

    headers = {
    "X-SBISSessionID": access_token,
    "Content-Type": "application/json",
    }  

    # _____________________________________________________________
    doc_id = []
    doc_type = []
    doc_number = []
    doc_full_name = []
    doc_data_main = []
    doc_at_created = []

    doc_counterparty_inn = []
    doc_counterparty_full_name = []

    doc_provider_inn = []
    doc_provider_full_name = []

    doc_assigned_manager = []
    doc_department = []

    inside_doc_author = []
    inside_doc_type = []
    inside_doc_item_full_doc_price_to = []
    inside_doc_item_full_doc_price_after = []

    inside_doc_item_note = []

    inside_doc_item_code = []
    inside_doc_item_article = []
    inside_doc_item_name = []

    inside_doc_item_quantity_to = []
    inside_doc_item_quantity_after = []
    inside_doc_item_unit_to = []
    inside_doc_item_unit_after = []

    inside_doc_item_price_to = []
    inside_doc_item_price_after = []
    inside_doc_item_full_item_price_to = []
    inside_doc_item_full_item_price_after = []
    

    # ___________________________________________________________________________________________

    var_status_has_more = "Да"
    i_page = 0

    while var_status_has_more == "Да":
        
        parameters_real = {
        "jsonrpc": "2.0",
        "method": "СБИС.СписокДокументов",
        "params": {
            "Фильтр": {
            "ДатаС": date_from,
            "ДатаПо": date_to,
            "Тип": "ReturnIn",
            # "Регламент": {
                # "Название": "Реализация"
            # },
            "Навигация": {
                "Страница": i_page
            }
            }
        },
        "id": 0
        }
        
        url_real = url_sbis_unloading
        
        response_points = requests.post(url_real, json=parameters_real, headers=headers)

        str_to_dict_points_main = json.loads(response_points.text)
        
        json_data_points = json.dumps(str_to_dict_points_main, ensure_ascii=False, indent=4).encode("utf8").decode()
        
        # with open("DICT_REALIZE.json", 'w') as json_file_points_o:
        #     json_file_points_o.write(json_data_points)
        
        j = 0
        for i in str_to_dict_points_main["result"]["Документ"]:
            # print(j)
            j += 1
            if (re.findall("возврат", i["Регламент"]["Название"].lower())[-1] == "возврат") and (i["Расширение"]["Проведен"].lower() == 'да'):
    # ___________________________________________________________________________________________
                
                try:
                    var_link = i["Идентификатор"]
                except:
                    var_link = np.nan
                    
                # print("var_link:", var_link)


                try:
                    doc_manager_first_name = str(i["Ответственный"]["Имя"])
                except:
                    doc_manager_first_name = ""
                try:
                    doc_manager_last_name = str(i["Ответственный"]["Фамилия"])
                except:
                    doc_manager_last_name = ""
                try:
                    doc_manager_surname_name = str(i["Ответственный"]["Отчество"])
                except:
                    doc_manager_surname_name = ""

                try:
                    doc_manager_name = " ".join([doc_manager_last_name, doc_manager_first_name, doc_manager_surname_name])
                except:
                    doc_manager_name = np.nan

                try:
                    var_doc_type = i["Регламент"]["Название"]
                except:
                    var_doc_type = np.nan

                try:
                    var_doc_number = i["Номер"] 
                except:
                    var_doc_number = np.nan

                try:
                    var_doc_full_name = i["Название"]
                except:
                    var_doc_full_name = np.nan

                try:
                    var_doc_data_main = i["Дата"]
                except:
                    var_doc_data_main = np.nan

                try:
                    var_doc_at_created = i["ДатаВремяСоздания"]
                except:
                    var_doc_at_created = np.nan
                try:
                    try:
                        var_doc_counterparty_inn = i["Контрагент"]["СвФЛ"]["ИНН"]
                        var_doc_counterparty_full_name = i["Контрагент"]["СвФЛ"]["НазваниеПолное"]
                    except:
                        var_doc_counterparty_inn = i["Контрагент"]["СвЮЛ"]["ИНН"]
                        var_doc_counterparty_full_name = i["Контрагент"]["СвЮЛ"]["НазваниеПолное"]
                except:
                    var_doc_counterparty_inn = np.nan
                    var_doc_counterparty_full_name = np.nan
                try:
                    try:
                        var_doc_provider_inn = i["НашаОрганизация"]["СвФЛ"]["ИНН"]
                        var_doc_provider_full_name = i["НашаОрганизация"]["СвФЛ"]["НазваниеПолное"]
                    except:
                        var_doc_provider_inn = i["НашаОрганизация"]["СвЮЛ"]["ИНН"]
                        var_doc_provider_full_name = i["НашаОрганизация"]["СвЮЛ"]["НазваниеПолное"]
                except:
                    var_doc_provider_inn = np.nan
                    var_doc_provider_full_name = np.nan

                try:
                    var_doc_assigned_manager = doc_manager_name
                except:
                    var_doc_assigned_manager = np.nan
                try:
                    var_doc_department = i["Подразделение"]["Название"]
                except:
                    var_doc_department = np.nan    
    # ___________________________________________________________________________________________

    # ___________________________________________________________________________________________

                parameters_real = {
                "jsonrpc": "2.0",
                "method": "СБИС.ПрочитатьДокумент",
                "params": {
                    "Документ": {
                        "Идентификатор": var_link,
                        "ДопПоля": "ДополнительныеПоля"
                    }
                },
                "id": 0
                }
            
                url_real = url_sbis_unloading
            
                response_points = requests.post(url_real, json=parameters_real, headers=headers)
                # print(response_points)
                # print(headers)
                str_to_dict_points = json.loads(response_points.text)
    # ___________________________________________________________________________________________
                
                # author_list = [str_to_dict_points["result"]["Автор"]["Имя"], str_to_dict_points["result"]["Автор"]["Фамилия"], str_to_dict_points["result"]["Автор"]["Отчество"]]
                try:
                    name = str_to_dict_points["result"]["Автор"]["Имя"]
                except:
                    name = ""
                try:
                    second_name = str_to_dict_points["result"]["Автор"]["Фамилия"]
                except:
                    second_name = ""
                try:
                    surname_name = str_to_dict_points["result"]["Автор"]["Отчество"]
                except:
                    surname_name = ""
            
                author_list = [name, second_name, surname_name]
                
                # print("автор:", " ".join(author_list).strip())
                try:
                    var_inside_doc_author = " ".join(author_list).strip()
                except:
                    var_inside_doc_author = np.nan
                
    # ___________________________________________________________________________________________
           
                def common_part_print():
                    print(j)
                    try:
                        print("var_link", var_link)
                    except:
                        print("var_link", np.nan)
                    try:
                        print("var_doc_type", i["Регламент"]["Название"])
                    except:
                        print("var_doc_type", np.nan)
                    try:
                        print("var_doc_number", i["Номер"])
                    except:
                        print("var_doc_number", np.nan)
                    try:
                        print("var_doc_full_name", i["Название"])
                    except:
                        print("var_doc_full_name", np.nan)

                    try:
                        print("var_doc_data_main", i["Дата"])
                    except:
                        print("var_doc_data_main", np.nan)
                    try:
                        print("var_doc_at_created", i["ДатаВремяСоздания"])
                    except:
                        print("var_doc_at_created", np.nan)
                    
                    try:
                        try:
                            print("var_doc_counterparty_inn", i["Контрагент"]["СвФЛ"]["ИНН"])
                            print("var_doc_counterparty_full_name", i["Контрагент"]["СвФЛ"]["НазваниеПолное"])
                        except:
                            print("var_doc_counterparty_inn", i["Контрагент"]["СвЮЛ"]["ИНН"])
                            print("var_doc_counterparty_full_name", i["Контрагент"]["СвЮЛ"]["НазваниеПолное"])  
                    except:
                        print("var_doc_counterparty_inn", np.nan)
                        print("var_doc_counterparty_full_name", np.nan)                          

                    try:
                        try:
                            print("var_doc_provider_inn", i["НашаОрганизация"]["СвФЛ"]["ИНН"])
                            print("var_doc_provider_full_name", i["НашаОрганизация"]["СвФЛ"]["НазваниеПолное"])
                        except:
                            print("var_doc_provider_inn", i["НашаОрганизация"]["СвЮЛ"]["ИНН"])
                            print("var_doc_provider_full_name", i["НашаОрганизация"]["СвЮЛ"]["НазваниеПолное"])
                    except:
                        print("var_doc_provider_inn", np.nan)
                        print("var_doc_provider_full_name", np.nan)                        
                
                    print("var_doc_assigned_manager", doc_manager_name)
                    try:
                        print("var_doc_department", i["Подразделение"]["Название"])
                    except:
                        print("var_doc_department", np.nan)

                    try:
                        print("автор:", " ".join(author_list).strip())
                    except:
                        print("автор:", np.nan)
                    
                # common_part_print()

    # ___________________________________________________________________________________________
                
                
                
                                
                attachments_id = {}
                try:
                    for l in range(len(str_to_dict_points["result"]["Вложение"])):
                        if str_to_dict_points["result"]["Вложение"][l]["Тип"].lower() in ("укдксчфдис", "укддис"):
                            # print(str_to_dict_points["result"]["Вложение"][l]["Тип"].lower())
                            attachments_id[str_to_dict_points["result"]["Вложение"][l]["Тип"].lower()] = str_to_dict_points["result"]["Вложение"][l]["Файл"]["Ссылка"]
                        else:
                            pass
                except:
                    pass
                
                try:
                    link_xml = list(attachments_id.values())[0]
                    a_xml = requests.get(link_xml, headers=headers)
                    a_xml.encoding = "cp1251"
                    xml_a_try = xmltodict.parse(a_xml.text)
                except:
                    print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                    try:                                
                        send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                        print('sent in sbis')
                        print('_____________')
                    except Exception as e:
                        print(e)
                        print('_____________')   
                
            
                                
    # ___________________________________________________________________________________________           
                if len(attachments_id) == 0:
                    
                    # print("ERROR len 0")
                    pass
    # ___________________________________________________________________________________________            
                elif len(attachments_id) > 0:

                    # print(attachments_id)
                
                        
                    for b in attachments_id.keys():
                        
                        
                        if  b == "укдксчфдис":   
                            
                                # _______________________________________________________________________________________________________________________________
                            a = requests.get(attachments_id["укдксчфдис"], headers=headers)
                            a.encoding = "cp1251"
                            # try:
                            # print(0)
                            xml_a = xmltodict.parse(a.text)    
                            # print(xml_a)
                            # _______________________________________________________________________________________________________________________________
                            var_inside_doc_type = "укдксчфдис"         
                            # _______________________________________________________________________________________________________________________________
                        
                            def_ukd_s_fdis(xml_a, var_inside_doc_author, var_inside_doc_type)
                        
                            # except:
                            # print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                            # try:                                
                            #     send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                            #     print('sent in sbis')
                            #     print('_____________')
                            # except Exception as e:
                            #     print(e)
                            #     print('_____________')  
                        
                        elif  b == "укддис":   
                            
                                # _______________________________________________________________________________________________________________________________
                            a = requests.get(attachments_id["укддис"], headers=headers)
                            a.encoding = "cp1251"
                            # try:
                            # print(0)
                            xml_a = xmltodict.parse(a.text)    
                            # print(xml_a)
                            # _______________________________________________________________________________________________________________________________
                            var_inside_doc_type = "укддис"         
                            # _______________________________________________________________________________________________________________________________
                        
                            def_ukd_s_fdis(xml_a, var_inside_doc_author, var_inside_doc_type)
                            
                            # except:
                            # print(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)                                
                            # try:                                
                            #     send_message(var_link, var_doc_number, var_doc_data_main, var_doc_type, var_doc_counterparty_inn)
                            #     print('sent in sbis')
                            #     print('_____________')
                            # except Exception as e:
                            #     print(e)
                            #     print('_____________')  
                                                 
                else:
                    print('')
                                        
        if var_status_has_more == "Нет":
            break
        elif str_to_dict_points_main["result"]["Навигация"]["ЕстьЕще"] == "Да":
            i_page += 1
        else:
            pass
        var_status_has_more = str_to_dict_points_main["result"]["Навигация"]["ЕстьЕще"]
        # print("ЕстьЕще", var_status_has_more)
        # print("___________________________________________________________________________________________________________________________________________________________")
        # print(f"СЛЕДУЮЩАЯ СТРАНИЦА {i_page}")       
            
            
    lst_append = [doc_id,
    doc_type,
    doc_number,
    doc_full_name,
    doc_data_main,
    doc_at_created,

    doc_counterparty_inn,
    doc_counterparty_full_name,

    doc_provider_inn,
    doc_provider_full_name,

    doc_assigned_manager,
    doc_department,

    inside_doc_author,
    inside_doc_type,
    inside_doc_item_full_doc_price_to,
    inside_doc_item_full_doc_price_after,

    inside_doc_item_note,

    inside_doc_item_code,
    inside_doc_item_article,
    inside_doc_item_name,

    inside_doc_item_quantity_to,
    inside_doc_item_quantity_after,
    inside_doc_item_unit_to,
    inside_doc_item_unit_after,

    inside_doc_item_price_to,
    inside_doc_item_price_after,
    inside_doc_item_full_item_price_to,
    inside_doc_item_full_item_price_after,
    ]

    lst_append_name = [
        "doc_id",
        "doc_type",
        "doc_number",
        "doc_full_name",
        "doc_data_main",
        "doc_at_created",
        # 6
        
        "doc_counterparty_inn",
        "doc_counterparty_full_name",
        # 8
        
        "doc_provider_inn",
        "doc_provider_full_name",
        # 10
        
        "doc_assigned_manager",
        "doc_department",
        # 12
        
        "inside_doc_author",
        "inside_doc_type",
        "inside_doc_item_full_doc_price_to",
        "inside_doc_item_full_doc_price_after",
        # 16
        
        "inside_doc_item_note",
        # 17
        
        "inside_doc_item_code",
        "inside_doc_item_article",
        "inside_doc_item_name",
        # 20

        "inside_doc_item_quantity_to",
        "inside_doc_item_quantity_after",
        "inside_doc_item_unit_to",
        "inside_doc_item_unit_after",
        # 24
        
        "inside_doc_item_price_to",
        "inside_doc_item_price_after",
        "inside_doc_item_full_item_price_to",
        "inside_doc_item_full_item_price_after"
        # 28
    ]    
   
    df = pd.DataFrame(columns=lst_append_name, data=list(zip(
    doc_id,
    doc_type,
    doc_number,
    doc_full_name,
    doc_data_main,
    doc_at_created,
    # 6

    doc_counterparty_inn,
    doc_counterparty_full_name,
    # 8
        
    doc_provider_inn,
    doc_provider_full_name,
    # 10

    doc_assigned_manager,
    doc_department,
    # 12

    inside_doc_author,
    inside_doc_type,
    inside_doc_item_full_doc_price_to,
    inside_doc_item_full_doc_price_after,
    # 16

    inside_doc_item_note,
    # 17

    inside_doc_item_code,
    inside_doc_item_article,
    inside_doc_item_name,
    # 20

    inside_doc_item_quantity_to,
    inside_doc_item_quantity_after,
    inside_doc_item_unit_to,
    inside_doc_item_unit_after,
    # 24

    inside_doc_item_price_to,
    inside_doc_item_price_after,
    inside_doc_item_full_item_price_to,
    inside_doc_item_full_item_price_after, 
    # 28
    )))
        

    # __________________________________________
    # __________________________________________
    # __________________________________________
    my_conn = create_engine(f"postgresql+psycopg2://{var_db_user_name}:{var_db_user_pass}@{var_db_host}:{var_db_port}/{var_db_name}")
    try: 
        my_conn.connect()
        print('my_conn.connect()')
        my_conn = my_conn.connect()
        df.to_sql(name=f'{name_unloading}', con=my_conn, if_exists="replace")
        print("df.sent()")
        my_conn.close()
        print("my_conn.close()")
    except:
        print('failed')
        print('my_conn.failed()')        
    # __________________________________________
    # __________________________________________
    # __________________________________________
            
sbis_real_processing_returnin(date_from_returnin, date_to_returnin, name_unloading) 

