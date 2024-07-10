from collections import defaultdict
import requests
import json

# Clickup Server info
HOST = 'app.clickup.com/api/v2/'
TOKEN = 'pk_4494199_KG040ILWM4SF4GU9LLZJPWQEVQXKIODV'
# Construct header for HTTP packet
REQUEST_HEADER = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
        "Authorization": TOKEN,
        "User-Agent": "pyclickup/0.1.4b",    
}

class TagEngine:

    def __init__(self,init_dict=None):
        if init_dict is not None:
            self.megaData = init_dict
        else:
            self.retrieved_list = True

            # Get the list of tubes
            #print("r1. Getting the list of tubes...")

            # r1.1 retrieve tubes under construction
            tubes_under_construction_tasks = [] #Deprecated, self.get_entire_list(31588960)
            # r1.2 retrieve tubes (the deployed ones)
            tube_tasks, tube_responses = self.get_entire_list(31662252)
            # r1.3 combine tubes lists
            self.all_device_tasks = tube_tasks+tubes_under_construction_tasks

            # Print status to console
            #if all([r.status_code == 200 for r in tube_responses]):
             #   print("r1. PASS -- 200")
            #else:
             #   self.retrieved_list = False
              #  print("r1. FAIL")


            # Get the list of cradles
            #print("r2. Getting the list of cradles...")

            # r2.1 retrieve cradles under construction
            cradles_under_construction_tasks = [] #Deprecated, self.get_entire_list(35861815)
            # r2.2 retrieve cradles (the deployed ones)
            cradle_tasks, cradle_responses = self.get_entire_list(37886759)
            # r2.3 combine cradles lists
            self.all_cradle_tasks = cradles_under_construction_tasks + cradle_tasks

            # Print status to console
            #if all([r.status_code == 200 for r in cradle_responses]):
             #   print("r2. PASS -- 200")
            #else:
             #   self.retrieved_list = False
              #  print("r2. FAIL")


            # Get the list of sites
            #print("r3. Getting the list of sites...")

            # r3.1 retrieve all tasks from the Deployment Sites folder         
            site_tasks, site_responses = self.get_all_folder_tasks(16867760)
            testing_site_tasks, testing_site_responses = self.get_all_folder_tasks(17089450)
            self.all_site_tasks = site_tasks + testing_site_tasks
            # r3.2 get list of deployed tubes
            # r3.3 get list of deployed cradles
            #print(all_site_tasks)

            # Print status to console
            if all([r.status_code == 200 for r in site_responses]) and all([r.status_code == 200 for r in testing_site_responses]):
                print("r3. PASS -- 200")
            else:
                self.retrieved_list = False
                print("r3. FAIL")

            if self.retrieved_list == False:
                return

            #create a dict of all the RFIDs and how they relate to each mount
            mapMountToRFID= {}
            for task in self.all_cradle_tasks:
                for custom_field in task['custom_fields']:
                    if custom_field['name']=="LF RFID" and 'value' in custom_field:
                        RFID = custom_field['value'][0]['name']                
                        mapMountToRFID[ task['name'] ] = RFID
                        break

            #
            # labels to tags
            #
            self.siteData = []
            clickupname_2_keys_dict = {
            #
            # ClickUp Field name <--> Influx Tag name
            #
                'plus code':'site_code',
                'node':'device',
                'cradle':'mount',
                'region':'region',
                'reporting agency':'reporting_agency',
                'watershed':'watershed',
            }

            # iterate through the Site Tasks
            for site_task in self.all_site_tasks:

                entry = {}
                entry["site_name"] = site_task["name"]
                entry["site_task_id"] = site_task["id"]

                # search through all custom fields 
                for custom_field in site_task['custom_fields']:

                    custom_field_name = custom_field['name'].lower()

                    # append all key,value pairs
                    if custom_field_name in clickupname_2_keys_dict:
                        tag_key = clickupname_2_keys_dict[custom_field_name]    

                        #
                        # - For Short Text and Numeric fields:
                        #   (e.g. PLUS Code, water depth)
                        #
                        #   Check if it is populated with a ['value'] 
                        #   and if so, add it to the {entry} dictionary
                        #
                        if custom_field['type'] == 'short_text' or custom_field['type'] == 'number':
                            if "value" in custom_field:
                                entry[tag_key] = custom_field['value']

                        #
                        # - For Drop Down fields:
                        #   (e.g. Watershed)
                        #
                        #   First, check if it is populated with a value. 
                        #
                        #   In this case, instead of a string or a number, 
                        #   the value is a Drop Down 'option' with additional metadata
                        #   
                        #   Extract the ['name'] from the selected option and add it to the {entry} dictionary
                        #
                        elif custom_field['type'] == 'drop_down':
                            if "value" in custom_field:
                                value = custom_field['value']
                                entry[tag_key] = custom_field['type_config']['options'][value]['name']

                        #
                        # - For Task fields :
                        #   (e.g. Node, Cradle)
                        #
                        #   The value is a list of 'tasks' each with additional metadata
                        #   The list may or may not be empty. 
                        #   
                        #   Iterate and extract each ['name'] and add it to the {entry} dictionary
                        #
                        elif custom_field['type'] == 'tasks':
                            if 'value' in custom_field:
                                tag_counter = 0
                                for value in custom_field['value']:
                                    tag_counter += 1
                                    if tag_counter == 1:
                                        entry[tag_key] = value['name']
                                    else:
                                        entry['{}_{}'.format(tag_key,tag_counter)] = value['name']


                        #
                        # - For Label fields :
                        #   (e.g. Agency, Region)
                        #
                        #   The value is a list of 'labels' each with additional metadata
                        #   The list may or may not be empty. 
                        #   
                        #   Iterate and extract each ['label'] and add it to the {entry} dictionary
                        #
                        elif custom_field['type'] == 'labels':
                            if 'value' in custom_field:
                                tag_counter = 0
                                for value in custom_field['value']:
                                    for option in custom_field['type_config']['options']:
                                        if value == option['id']:
                                            tag_counter += 1
                                            if tag_counter == 1:
                                                entry[tag_key] = option['label']
                                            else:
                                                entry['{}_{}'.format(tag_key,tag_counter)] = option['label']

                        #
                        # - For other fields :
                        #   Print it to the log
                        #
                        else:
                            print(custom_field)

                if "mount" in entry and entry["mount"] in mapMountToRFID:
                    entry["rfid"] = mapMountToRFID[entry["mount"]]

                self.siteData.append(entry)

            # Remove dependency on pandas
            # self.siteDataFrame = pd.DataFrame.from_records(siteData)
            # print(siteDataFrame)
            


            #
            # Device Data -- labels to tags
            #        
            #index = ["device","meid","method"]
            self.deviceData = []
            clickupname_2_keys_dict = {
            #
            # ClickUp Field name <--> Influx Tag name
            #
                'cell module':'device_meid',
                'maxbotix model':'device_method',
                'commit hash':'desired_commit_hash',
            }   
            # iterate through the Device Tasks         
            for device_task in self.all_device_tasks:
                entry = {}
                entry["device"] = device_task["name"]
                entry["device_status"] = device_task["status"]["status"]
                
                # search through all custom fields 
                for custom_field in device_task['custom_fields']:

                    custom_field_name = custom_field['name'].lower()   

                    # append all key,value pairs
                    if custom_field_name in clickupname_2_keys_dict:
                        tag_key = clickupname_2_keys_dict[custom_field_name]    

                        #
                        # - For Drop Down fields:
                        #   (e.g. MaxBotix Model, Firmware)
                        #
                        #   First, check if it is populated with a value. 
                        #
                        #   In this case, instead of a string or a number, 
                        #   the value is a Drop Down 'option' with additional metadata
                        #   
                        #   Extract the ['name'] from the selected option and add it to the {entry} dictionary
                        #
                        if custom_field['type'] == 'drop_down':
                            if "value" in custom_field:
                                value = custom_field['value']
                                entry[tag_key] = custom_field['type_config']['options'][value]['name']

                        #
                        # - For Task fields :
                        #   (e.g. Firmware)
                        #
                        #   The value is a list of 'tasks' each with additional metadata
                        #   The list may or may not be empty. 
                        #   
                        #   Iterate and extract each ['name'] and add it to the {entry} dictionary
                        #
                        elif custom_field['type'] == 'tasks':
                            if 'value' in custom_field:
                                tag_counter = 0
                                for value in custom_field['value']:
                                    tag_counter += 1
                                    if tag_counter == 1:
                                        entry[tag_key] = value['name']
                                    else:
                                        entry['{}_{}'.format(tag_key,tag_counter)] = value['name']

                        #
                        # - For other fields :
                        #   Print it to the log
                        #
                        else:
                            print(custom_field)                                                                

                self.deviceData.append(entry)

            #print(deviceData)

            # Remove dependency on pandas
            # self.deviceDataFrame = pd.DataFrame.from_records(deviceData)
            #print(deviceDataFrame)

            # merge all the results into one data frame
            # self.megaframe = self.siteDataFrame.merge(self.deviceDataFrame,on=['device'],how='right')

            # a mega list of all the results, where each result is a dictionary of metadata 
            # - https://stackoverflow.com/questions/5501810/join-two-lists-of-dictionaries-on-a-single-key        
            d = defaultdict(dict)
            for l in (self.siteData, self.deviceData):
                for elem in l:
                    if 'device' in elem:
                        d[elem['device']].update(elem)
            self.megaData = d.values()        

            # If we'd like to have entries sorted by device name:
            # - https://stackoverflow.com/questions/72899/how-do-i-sort-a-list-of-dictionaries-by-a-value-of-the-dictionary
            # self.sorted_megaData = sorted(megaData, key=itemgetter("device"))

    def get_list(self,list_id, page_count=0):
        # retrieve all tasks for a given [list_id]
        #print(f"get_list({list_id,page_count})")
        r = requests.get('https://{}/list/{}/task?page={}&archived=false'.format(HOST,list_id,page_count),headers = REQUEST_HEADER)
        task_page = []

        if r.status_code == 200:
            if 'tasks' in json.loads(r.content):
                task_page = json.loads(r.content)['tasks']
            else:
                print("TagEngine() - Error - get_list({},page={}): HTTP {} -- No 'tasks' found. -- {}".format(list_id,page_count,r.status_code,r.content))
        else:
            print("TagEngine() - Error - get_list({},page={}): HTTP {} -- {}".format(list_id,page_count,r.status_code,r.content))
        return task_page, r
 
    def get_entire_list(self,list_id, page_start=0, page_limit=-1):    
        tasks = []
        responses = []
        page_count = page_start
        
        task_page,r = self.get_list(list_id, page_count)
        tasks += task_page
        responses.append(r)

        while task_page and (page_limit < 0 or page_count < page_limit):
            page_count += 1
            task_page,r = self.get_list(list_id, page_count)
            tasks += task_page
            responses.append(r)
            
        return tasks,responses
    
    def get_all_folder_tasks(self,folder_id):
        site_list_ids = []
        tasks = []
        responses = []

        r = requests.get('https://{}/folder/{}'.format(HOST,folder_id),headers = REQUEST_HEADER)
        responses.append(r)

        if r.status_code == 200 and 'lists' in json.loads(r.content):
            for site_list in json.loads(r.content)['lists']:
                #print(json.dumps(list,indent=4))
                #print("{} ({})".format(list["name"],list["id"]))
                site_list_ids.append(site_list["id"])   
                
            for list_id in site_list_ids:
                tmp_tasks, tmp_responses = self.get_entire_list(list_id)
                tasks += tmp_tasks
                responses += tmp_responses
        else:
            print("TagEngine() - Error - get_all_folder_tasks(): Unable to retrieve all lists in Folder {} -- HTTP {} -- {}".format(
                folder_id,
                r.status_code,
                r.content))
                
        return tasks, responses

    '''
    def get_info_as_json(self,key,value):
        try:
            loc = self.megaframe.loc[self.megaframe[key] == value]
            return loc.iloc[0].to_json()
        except:
            return "{}"
    '''

    '''
    #
    # get_info_as_dict(self,key,value):
    # 

    Returns the dictionary entry of metadata, for example:
        {'site_name': 'Torrey Road,R78F+4Q4',
        'site_task_id': 'cmpd2y',
        'mount': 'bma32040rory',
        'device': 'mpa27197marv',
        'site_code': '86JRR78F+4Q4',
        'region': 'US-MI',
        'reporting_agency': 'Hyfi',
        'watershed': 'Saginaw',
        'rfid': '000C0B7F94EC',
        'device_meid': '354328092088431',
        'desired_commit_hash': '3d19130',
        'device_method': 'MB7388-100'}  

    If no match found, return an empty dictionary
    '''
    def get_info_as_dict(self,key,value):
    # If the Match list is non-empty, return the first result in the list
        '''        
        match_list = list( filter(lambda entry: key in entry and entry[key] == value, self.megaData) )

        match = {}
        if match_list: 
            match = match_list[0]

        return match
        '''
        # - https://stackoverflow.com/questions/8653516/python-list-of-dictionaries-search
        # - https://stackoverflow.com/questions/38865201/most-efficient-way-to-search-in-list-of-dicts 
        return next( filter(lambda entry: key in entry and entry[key] == value, self.megaData), {})

    def get_info_as_tag_string(self,key,value,extra_dict={},escape_spaces_and_commas=True):

        # Get all ClickUp info
        clickup_dict = self.get_info_as_dict(key,value)

        # Append any additional info outside of ClickUp (e.g., tag entries sent by the device)
        # -- Note: if both {clickup_dict} and {extra_dict} have an entry for the same key, 
        #          the entry in {extra_dict} is used
        #


        info_dict =  {**clickup_dict,**extra_dict}
        info_dict["rfid_match"] = "0"

        if "measured_rfid" in extra_dict:
            if "rfid" in clickup_dict:
                if extra_dict["measured_rfid"] == clickup_dict["rfid"]:
                    info_dict["rfid_match"] = "1"



        # Convert all key-value pairs to a string of tags compatible with Influx
        if escape_spaces_and_commas:
            tag_string = ','.join(['{}={}'.format(key,info_dict[key].replace(" ","\ ").replace(",","\,")) for key in info_dict if info_dict[key] != None])
        else:
            tag_string = ','.join(['{}={}'.format(key,info_dict[key]) for key in info_dict if info_dict[key] != None])
        return tag_string   

    def getAllCradleTasks(self):
        if hasattr(self,'all_cradle_tasks'):
            return self.all_cradle_tasks

    def getAllDeviceTasks(self):
        if hasattr(self,'all_device_tasks'):
            return self.all_device_tasks

    def getAllSiteTasks(self):
        if hasattr(self,'all_site_tasks'):
            return self.all_site_tasks

    """
    def getDeviceInfo(self,key,value):
        try:
            loc = self.deviceDataFrame.loc[self.deviceDataFrame[key] == value]
            return loc.iloc[0].to_json(orient="index")#return the first thing we found
        except:
            return "{}"
    """

    """
    def getSiteInfo(self,key,value):
        try:
            loc = self.siteDataFrame.loc[self.siteDataFrame[key] == value]
            return loc.iloc[0].to_json(orient="index")#return the first thing we found
        except:
            return "{}"         
    """

if __name__ == '__main__':
    import time
    time_start = time.time()
    assets = TagEngine()
    #import json
    #print(json.loads(assets.getInfo("rfid","asd")))
    #examples
    print("Device by meid: ", assets.getDeviceInfo("device_meid","354328092080990"))
    print("Site by rfid: ", assets.getSiteInfo("rfid","007791497CD3"))
    print("Site by Mount: bma32006rory", assets.getSiteInfo("mount","bma32006rory"))
    print("Site by wrong RFID (should return empty): ", assets.getSiteInfo("rfid","asd"))
    print("Site by name: ", assets.getSiteInfo("site_name","Bassmint, R7"))
    print("Total Query Time: ", time.time()-time_start)