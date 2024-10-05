import datetime
import json
from time import timezone
from flask import Blueprint, jsonify, request
import hashlib
import bisect
import pandas as pd

from DBCLients.cockroach_client import CockroachClient

agent = Blueprint('agentAllocation', __name__)


COUNTER={
    "1":0,
    "2":0,
    "3":0,
    "4":0
}

@agent.route('/agentAllocation', methods=['POST'])
def agent_allocation():
    return agent_allocation_helper(request.json)

def agent_allocation_helper(request):
    # no action of lead already exists in mapping
    cdb=CockroachClient()
    cdb.connect()
    rec=cdb.fetch_all('''select * from agent_score where grade = %s and team_name=%s order by grade_ranking desc;''',(request["grade"],request["team"]))
    print(rec)
    cdb.close()
    n=len(rec)
    ls=[]
    for entry in rec:
        dict={}
        dict["employeeid"]=entry[0]
        dict["username"]=entry[1]
        dict["grade"]=entry[-3]
        dict["bucket"]=entry[-1]
        dict["team"]=request["team"]
        dict["leadid"]=request["leadid"]
        ls.append(dict)
        # if(int(entry[-1])==0):
        #     allocate_lead(dict)
        #     update_agent(dict)
        #     COUNTER[dict["grade"]]=COUNTER[dict["grade"]]+1
            
        # elif(COUNTER[dict["grade"]]%n>0):
        #     allocate_lead(dict)
        #     update_agent(dict)
        #     COUNTER[dict["grade"]]=COUNTER[dict["grade"]]+1
        
        ch=ConsistentHashing()
        ch.add_node(dict)
    print(dict)
    allocate_lead(ls[COUNTER[str(dict["grade"])]%n])
    update_agent(ls[COUNTER[str(dict["grade"])]%n])
    COUNTER[str(dict["grade"])]=COUNTER[str(dict["grade"])]+1  
    print(ls)
    return jsonify("lead allocated"), 201


def allocate_lead(dict):
    cdb=CockroachClient()
    cdb.connect()
    cdb.execute_query("""
            INSERT INTO lead_mapping (
                lead_id,
                employeeid,
                team,
                created_on 
            ) VALUES (%s, %s,%s,%s)
        """, (
            dict['leadid'],
            dict['employeeid'],
            dict['team'],
            datetime.datetime.now(datetime.timezone.utc)
        ))
    cdb.close()
    
def update_agent(dict):
    cdb=CockroachClient()
    cdb.connect()
    cdb.execute_query("""
            update agent_score set bucket=%s where employeeid=%s
        """, (
            dict['bucket']+1,
            dict['employeeid']
        ))
    cdb.close()
    
    
    
@agent.route('/getallagents', methods=['GET']) 
def get_all_agents():
    id = request.args.get('agentId')
    cdb=CockroachClient()
    cdb.connect()
    rec=cdb.fetch_all('''select * from agent_score ags join lead_mapping lm on lm.employeeid=ags.employeeid where ags.employeeid='''+"'"+id.upper()+"'")
    cdb.close()
    return jsonify(rec), 201

@agent.route('/GetAllLeads/', methods=['GET']) 
def get_leads_for_agent():
    id = request.args.get('agentId')
    print(type(id))
    cdb=CockroachClient()
    cdb.connect()
    print('''select * from lead_mapping where employeeid='''+id.upper())
    rec=cdb.fetch_all('''select * from lead_mapping where employeeid='''+"'"+id.upper()+"'")
    cdb.close()
    return jsonify(rec), 201
    
@agent.route('/GetLeadMapping/', methods=['GET']) 
def get_leads_mapping():
    cdb=CockroachClient()
    cdb.connect()
    print('''select * from lead_mapping''')
    rec=cdb.fetch_all('''select * from lead_mapping''')
    cdb.close()
    return jsonify(rec), 201

@agent.route('/updateLeadStatus/', methods=['POST']) 
def update_Lead_Status():
    cdb=CockroachClient()
    cdb.connect()
    #print('''select * from lead_mapping''')
    rec=cdb.execute_query('''update lead_mapping set status='''+"'"+request.json["status"]+"where lead_id="+request.json["lead_id"])
    cdb.close()
    return jsonify(rec), 201

@agent.route('/JsonToTable', methods=['POST'])
def save_json():
    json1 = request.json
    i=0
#     cdb=CockroachClient()
#     cdb.connect()
#     cdb.execute_query('''CREATE TABLE agent_score (
# 	employeeid VARCHAR(20),
#     employee_name VARCHAR(50),
# 	team_name VARCHAR(20),
# 	grade int NOT NULL,
# 	grade_ranking int NOT NULL,
#     bucket int default 0
# );''')
#     cdb.close()
#     return jsonify("lead allocated"), 201
    for entry in json.loads(data):
        cdb=CockroachClient()
        cdb.connect()
        grade=""
        if(i%4==0):
            grade="1"
        if(i%4==1):
            grade="2"
        if(i%4==2):
            grade="3"
        if(i%4==3):
            grade="4"

        cdb.execute_query("""
            INSERT INTO agent_score (
                employeeid,
                username_y,
                Total_Leads,
                Grade,
                Matrix_Group_y 
            ) VALUES (%s, %s, %s, %s, %s)
        """, (
            entry['employeeid'],
            entry['username_y'],
            entry['Total_Leads'],
            grade,
            entry['Matrix_Group_y']
        ))
        cdb.close()
        i+=1
    return jsonify("lead allocated"), 201

@agent.route('/ExcelToTable', methods=['POST'])
def upload_excel_to_db():
    df = pd.read_excel('July24_AgentGradeModelData.xlsx')
    cdb=CockroachClient()
    cdb.connect()

    for index, row in df.iterrows():
        cdb.execute_query("""
            INSERT INTO agent_score (employeeid, employee_name, team_name, grade, grade_ranking, bucket)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (row['employeeid'], row['employee_name'], row['team_name'], 
              row['grade'], row['grade_ranking'], row.get('bucket', 0)))

    cdb.close()

class ConsistentHashing:
    def __init__(self, replicas=3):
        self.replicas = replicas  # Number of virtual nodes for each real node ,  replicas=n*k, k=log(n)
        self.ring = []            # Sorted list of hashed values (the hash ring)
        self.nodes = {}           # Dictionary mapping hashed values to nodes

    def _hash(self, key):
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def add_node(self, node):
        for i in range(self.replicas):
            virtual_node = f"{node}::{i}"
            hashed_value = self._hash(virtual_node)
            self.ring.append(hashed_value)
            self.nodes[hashed_value] = node
        self.ring.sort()  # Keep the ring sorted for binary search

    def add_nodes_from_client(self, cockroach_client):
        """Add all nodes fetched from the CockroachDB client."""
        nodes = cockroach_client.fetch_nodes()
        if isinstance(nodes, list):
            for node in nodes:
                self.add_node(node)
        else:
            print(nodes)  # Print error if fetching nodes failed

    def remove_node(self, node):
        """Remove a node and its virtual nodes from the hash ring."""
        for i in range(self.replicas):
            virtual_node = f"{node}::{i}"
            hashed_value = self._hash(virtual_node)
            if hashed_value in self.nodes:
                self.ring.remove(hashed_value)
                del self.nodes[hashed_value]

    def get_node(self, key):
        """Get the closest node for a given key."""
        if not self.ring:
            return None
        
        hashed_key = self._hash(key)
        idx = bisect.bisect(self.ring, hashed_key)
        if idx == len(self.ring):
            idx = 0
        return self.nodes[self.ring[idx]]

data='''
[
    {
        "employeeid": "BPW12398",
        "username_y": "Kunal Goswami",
        "Total_Leads": 11,
        "Total_Booked": 0,
        "Total_IssuedLead": 0,
        "Total_APE": 0,
        "Total_Attempts": 17,
        "Total_Connects": 0,
        "Total_TalkTime": 178,
        "Conversion_Percentage_MTD": 0,
        "APE_per_lead_MTD": 0,
        "ATS_MTD": null,
        "Issuance_MTD": null,
        "Attempts_per_lead_MTD": 1.545454545,
        "Connectivity_MTD": 0,
        "TalkTime_per_Lead_MTD": 16.18181818,
        "Total_Leads_Dec_LastWeek": null,
        "Total_Booked_Dec_LastWeek": null,
        "Total_APE_Dec_LastWeek": null,
        "Conversion_Percentage_Dec_LastWeek": null,
        "APE_per_Lead_Dec_LastWeek": null,
        "ATS_Dec_LastWeek": null,
        "Ecode_x": "PW00010",
        "Location_x": "Gurgaon",
        "Ageing_x": "12 & Above",
        "Status_x": "Active",
        "Process_x": "HDFC_APE",
        "Matrix_Group_x": "Hexspeak",
        "Ecode_y": "PW00010",
        "Location_y": "Gurgaon",
        "Ageing_y": "12 & Above",
        "Status_y": "Active",
        "Process_y": "HDFC_APE",
        "Matrix_Group_y": "Hexspeak"
    },
    {
        "employeeid": "BPW17854",
        "username_y": "Manju Kumari",
        "Total_Leads": 7,
        "Total_Booked": 0,
        "Total_IssuedLead": 0,
        "Total_APE": 0,
        "Total_Attempts": 2,
        "Total_Connects": 1,
        "Total_TalkTime": 422,
        "Conversion_Percentage_MTD": 0,
        "APE_per_lead_MTD": 0,
        "ATS_MTD": null,
        "Issuance_MTD": null,
        "Attempts_per_lead_MTD": 0.2857142857,
        "Connectivity_MTD": 50,
        "TalkTime_per_Lead_MTD": 60.28571429,
        "Total_Leads_Dec_LastWeek": null,
        "Total_Booked_Dec_LastWeek": null,
        "Total_APE_Dec_LastWeek": null,
        "Conversion_Percentage_Dec_LastWeek": null,
        "APE_per_Lead_Dec_LastWeek": null,
        "ATS_Dec_LastWeek": null,
        "Ecode_x": "PW00668",
        "Location_x": "Gurgaon",
        "Ageing_x": "12 & Above",
        "Status_x": "Active",
        "Process_x": "HDFC_APE",
        "Matrix_Group_x": "Hexspeak",
        "Ecode_y": "PW00668",
        "Location_y": "Gurgaon",
        "Ageing_y": "12 & Above",
        "Status_y": "Active",
        "Process_y": "HDFC_APE",
        "Matrix_Group_y": "Hexspeak"
    },
    {
        "employeeid": "PM28966",
        "username_y": "riya gupta",
        "Total_Leads": 1,
        "Total_Booked": 0,
        "Total_IssuedLead": 0,
        "Total_APE": 0,
        "Total_Attempts": 0,
        "Total_Connects": 0,
        "Total_TalkTime": 0,
        "Conversion_Percentage_MTD": 0,
        "APE_per_lead_MTD": 0,
        "ATS_MTD": null,
        "Issuance_MTD": null,
        "Attempts_per_lead_MTD": 0,
        "Connectivity_MTD": null,
        "TalkTime_per_Lead_MTD": 0,
        "Total_Leads_Dec_LastWeek": null,
        "Total_Booked_Dec_LastWeek": null,
        "Total_APE_Dec_LastWeek": null,
        "Conversion_Percentage_Dec_LastWeek": null,
        "APE_per_Lead_Dec_LastWeek": null,
        "ATS_Dec_LastWeek": null,
        "Ecode_x": null,
        "Location_x": null,
        "Ageing_x": null,
        "Status_x": null,
        "Process_x": null,
        "Matrix_Group_x": null,
        "Ecode_y": null,
        "Location_y": null,
        "Ageing_y": null,
        "Status_y": null,
        "Process_y": null,
        "Matrix_Group_y": null
    },
    {
        "employeeid": "PT02928",
        "username_y": "Nav Test",
        "Total_Leads": 6,
        "Total_Booked": 0,
        "Total_IssuedLead": 0,
        "Total_APE": 0,
        "Total_Attempts": 5,
        "Total_Connects": 1,
        "Total_TalkTime": 781,
        "Conversion_Percentage_MTD": 0,
        "APE_per_lead_MTD": 0,
        "ATS_MTD": null,
        "Issuance_MTD": null,
        "Attempts_per_lead_MTD": 0.8333333333,
        "Connectivity_MTD": 20,
        "TalkTime_per_Lead_MTD": 130.1666667,
        "Total_Leads_Dec_LastWeek": null,
        "Total_Booked_Dec_LastWeek": null,
        "Total_APE_Dec_LastWeek": null,
        "Conversion_Percentage_Dec_LastWeek": null,
        "APE_per_Lead_Dec_LastWeek": null,
        "ATS_Dec_LastWeek": null,
        "Ecode_x": null,
        "Location_x": null,
        "Ageing_x": null,
        "Status_x": null,
        "Process_x": null,
        "Matrix_Group_x": null,
        "Ecode_y": null,
        "Location_y": null,
        "Ageing_y": null,
        "Status_y": null,
        "Process_y": null,
        "Matrix_Group_y": null
    },
    {
        "employeeid": "PW00010",
        "username_y": "Archana Pandey",
        "Total_Leads": 795,
        "Total_Booked": 79,
        "Total_IssuedLead": 18,
        "Total_APE": 2764250.28,
        "Total_Attempts": 10570,
        "Total_Connects": 358,
        "Total_TalkTime": 454428,
        "Conversion_Percentage_MTD": 9.937106918,
        "APE_per_lead_MTD": 3477.044377,
        "ATS_MTD": 34990.50987,
        "Issuance_MTD": 22.78481013,
        "Attempts_per_lead_MTD": 13.29559748,
        "Connectivity_MTD": 3.386944182,
        "TalkTime_per_Lead_MTD": 571.6075472,
        "Total_Leads_Dec_LastWeek": 117,
        "Total_Booked_Dec_LastWeek": 11,
        "Total_APE_Dec_LastWeek": 511908,
        "Conversion_Percentage_Dec_LastWeek": 9.401709402,
        "APE_per_Lead_Dec_LastWeek": 4375.282051,
        "ATS_Dec_LastWeek": 46537.09091,
        "Ecode_x": "PW00010",
        "Location_x": "Gurgaon",
        "Ageing_x": "12 & Above",
        "Status_x": "Active",
        "Process_x": "HDFC_APE",
        "Matrix_Group_x": "Hexspeak",
        "Ecode_y": "PW00010",
        "Location_y": "Gurgaon",
        "Ageing_y": "12 & Above",
        "Status_y": "Active",
        "Process_y": "HDFC_APE",
        "Matrix_Group_y": "Hexspeak"
    },
    {
        "employeeid": "PW00668",
        "username_y": "vivek shukla",
        "Total_Leads": 3,
        "Total_Booked": 1,
        "Total_IssuedLead": 0,
        "Total_APE": 16277.49,
        "Total_Attempts": 11,
        "Total_Connects": 0,
        "Total_TalkTime": 382,
        "Conversion_Percentage_MTD": 33.33333333,
        "APE_per_lead_MTD": 5425.83,
        "ATS_MTD": 16277.49,
        "Issuance_MTD": 0,
        "Attempts_per_lead_MTD": 3.666666667,
        "Connectivity_MTD": 0,
        "TalkTime_per_Lead_MTD": 127.3333333,
        "Total_Leads_Dec_LastWeek": null,
        "Total_Booked_Dec_LastWeek": null,
        "Total_APE_Dec_LastWeek": null,
        "Conversion_Percentage_Dec_LastWeek": null,
        "APE_per_Lead_Dec_LastWeek": null,
        "ATS_Dec_LastWeek": null,
        "Ecode_x": null,
        "Location_x": null,
        "Ageing_x": null,
        "Status_x": null,
        "Process_x": null,
        "Matrix_Group_x": null,
        "Ecode_y": null,
        "Location_y": null,
        "Ageing_y": null,
        "Status_y": null,
        "Process_y": null,
        "Matrix_Group_y": null
    },
    {
        "employeeid": "PW00400",
        "username_y": "Priti sharma",
        "Total_Leads": 10,
        "Total_Booked": 0,
        "Total_IssuedLead": 0,
        "Total_APE": 0,
        "Total_Attempts": 0,
        "Total_Connects": 0,
        "Total_TalkTime": 0,
        "Conversion_Percentage_MTD": 0,
        "APE_per_lead_MTD": 0,
        "ATS_MTD": null,
        "Issuance_MTD": null,
        "Attempts_per_lead_MTD": 0,
        "Connectivity_MTD": null,
        "TalkTime_per_Lead_MTD": 0,
        "Total_Leads_Dec_LastWeek": null,
        "Total_Booked_Dec_LastWeek": null,
        "Total_APE_Dec_LastWeek": null,
        "Conversion_Percentage_Dec_LastWeek": null,
        "APE_per_Lead_Dec_LastWeek": null,
        "ATS_Dec_LastWeek": null,
        "Ecode_x": null,
        "Location_x": null,
        "Ageing_x": null,
        "Status_x": null,
        "Process_x": null,
        "Matrix_Group_x": null,
        "Ecode_y": null,
        "Location_y": null,
        "Ageing_y": null,
        "Status_y": null,
        "Process_y": null,
        "Matrix_Group_y": null
    },
    {
        "employeeid": "PZ00024",
        "username_y": "Test User",
        "Total_Leads": 4,
        "Total_Booked": 0,
        "Total_IssuedLead": 0,
        "Total_APE": 0,
        "Total_Attempts": 0,
        "Total_Connects": 0,
        "Total_TalkTime": 0,
        "Conversion_Percentage_MTD": 0,
        "APE_per_lead_MTD": 0,
        "ATS_MTD": null,
        "Issuance_MTD": null,
        "Attempts_per_lead_MTD": 0,
        "Connectivity_MTD": null,
        "TalkTime_per_Lead_MTD": 0,
        "Total_Leads_Dec_LastWeek": null,
        "Total_Booked_Dec_LastWeek": null,
        "Total_APE_Dec_LastWeek": null,
        "Conversion_Percentage_Dec_LastWeek": null,
        "APE_per_Lead_Dec_LastWeek": null,
        "ATS_Dec_LastWeek": null,
        "Ecode_x": null,
        "Location_x": null,
        "Ageing_x": null,
        "Status_x": null,
        "Process_x": null,
        "Matrix_Group_x": null,
        "Ecode_y": null,
        "Location_y": null,
        "Ageing_y": null,
        "Status_y": null,
        "Process_y": null,
        "Matrix_Group_y": null
    },
    {
        "employeeid": "PW00667",
        "username_y": "Priti sharma",
        "Total_Leads": 7,
        "Total_Booked": 0,
        "Total_IssuedLead": 0,
        "Total_APE": 0,
        "Total_Attempts": 0,
        "Total_Connects": 0,
        "Total_TalkTime": 0,
        "Conversion_Percentage_MTD": 0,
        "APE_per_lead_MTD": 0,
        "ATS_MTD": null,
        "Issuance_MTD": null,
        "Attempts_per_lead_MTD": 0,
        "Connectivity_MTD": null,
        "TalkTime_per_Lead_MTD": 0,
        "Total_Leads_Dec_LastWeek": null,
        "Total_Booked_Dec_LastWeek": null,
        "Total_APE_Dec_LastWeek": null,
        "Conversion_Percentage_Dec_LastWeek": null,
        "APE_per_Lead_Dec_LastWeek": null,
        "ATS_Dec_LastWeek": null,
        "Ecode_x": null,
        "Location_x": null,
        "Ageing_x": null,
        "Status_x": null,
        "Process_x": null,
        "Matrix_Group_x": null,
        "Ecode_y": null,
        "Location_y": null,
        "Ageing_y": null,
        "Status_y": null,
        "Process_y": null,
        "Matrix_Group_y": null
    },
    {
        "employeeid": "PW00040",
        "username_y": "raj sharma",
        "Total_Leads": 1,
        "Total_Booked": 0,
        "Total_IssuedLead": 0,
        "Total_APE": 0,
        "Total_Attempts": 0,
        "Total_Connects": 0,
        "Total_TalkTime": 0,
        "Conversion_Percentage_MTD": 0,
        "APE_per_lead_MTD": 0,
        "ATS_MTD": null,
        "Issuance_MTD": null,
        "Attempts_per_lead_MTD": 0,
        "Connectivity_MTD": null,
        "TalkTime_per_Lead_MTD": 0,
        "Total_Leads_Dec_LastWeek": null,
        "Total_Booked_Dec_LastWeek": null,
        "Total_APE_Dec_LastWeek": null,
        "Conversion_Percentage_Dec_LastWeek": null,
        "APE_per_Lead_Dec_LastWeek": null,
        "ATS_Dec_LastWeek": null,
        "Ecode_x": null,
        "Location_x": null,
        "Ageing_x": null,
        "Status_x": null,
        "Process_x": null,
        "Matrix_Group_x": null,
        "Ecode_y": null,
        "Location_y": null,
        "Ageing_y": null,
        "Status_y": null,
        "Process_y": null,
        "Matrix_Group_y": null
    },
    {
        "employeeid": "PW00015",
        "username_y": "harshit sharma",
        "Total_Leads": 2,
        "Total_Booked": 0,
        "Total_IssuedLead": 0,
        "Total_APE": 0,
        "Total_Attempts": 0,
        "Total_Connects": 0,
        "Total_TalkTime": 0,
        "Conversion_Percentage_MTD": 0,
        "APE_per_lead_MTD": 0,
        "ATS_MTD": null,
        "Issuance_MTD": null,
        "Attempts_per_lead_MTD": 0,
        "Connectivity_MTD": null,
        "TalkTime_per_Lead_MTD": 0,
        "Total_Leads_Dec_LastWeek": null,
        "Total_Booked_Dec_LastWeek": null,
        "Total_APE_Dec_LastWeek": null,
        "Conversion_Percentage_Dec_LastWeek": null,
        "APE_per_Lead_Dec_LastWeek": null,
        "ATS_Dec_LastWeek": null,
        "Ecode_x": null,
        "Location_x": null,
        "Ageing_x": null,
        "Status_x": null,
        "Process_x": null,
        "Matrix_Group_x": null,
        "Ecode_y": null,
        "Location_y": null,
        "Ageing_y": null,
        "Status_y": null,
        "Process_y": null,
        "Matrix_Group_y": null
    },
    {
        "employeeid": "PW00012",
        "username_y": "Ravi Kumar",
        "Total_Leads": 3,
        "Total_Booked": 0,
        "Total_IssuedLead": 0,
        "Total_APE": 0,
        "Total_Attempts": 0,
        "Total_Connects": 0,
        "Total_TalkTime": 0,
        "Conversion_Percentage_MTD": 0,
        "APE_per_lead_MTD": 0,
        "ATS_MTD": null,
        "Issuance_MTD": null,
        "Attempts_per_lead_MTD": 0,
        "Connectivity_MTD": null,
        "TalkTime_per_Lead_MTD": 0,
        "Total_Leads_Dec_LastWeek": null,
        "Total_Booked_Dec_LastWeek": null,
        "Total_APE_Dec_LastWeek": null,
        "Conversion_Percentage_Dec_LastWeek": null,
        "APE_per_Lead_Dec_LastWeek": null,
        "ATS_Dec_LastWeek": null,
        "Ecode_x": null,
        "Location_x": null,
        "Ageing_x": null,
        "Status_x": null,
        "Process_x": null,
        "Matrix_Group_x": null,
        "Ecode_y": null,
        "Location_y": null,
        "Ageing_y": null,
        "Status_y": null,
        "Process_y": null,
        "Matrix_Group_y": null
    }
]'''