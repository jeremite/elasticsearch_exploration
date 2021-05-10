def create_index(client,index_name,stop_words):
    """Creates an index in Elasticsearch if one isn't already there."""
    client.indices.create(
        index=index_name, 
        body={
        "settings": {"index":{"number_of_shards": 1},
        ## custom analyser for org names
                    "analysis": {
      "analyzer": {
        "name_stopwords": {
          "type": "standard",
          "stopwords": stop_words#["the","of","co","inc","Corp","corporation","Limited","LLC","LLP","Ltd"]
        }
      }
     }
                    },

        "mappings":{
            "properties":{
                "join_field": {
                  "type": "join",
                  "relations": {
                    "orgs": "sites"
                  }
                },
                "site_uid": {
                  "store": True,
                  "type": "keyword"
                },
                "site_address": {
                  "term_vector": "with_positions_offsets",
                  "store": True,
                  "type": "text",
                  #"null_value": "NULL"
                },
                "site_name": {
                  "term_vector": "with_positions_offsets",
                  "store": True,
                  "type": "text",
                  "analyzer": "name_stopwords",
                  "fielddata": True,
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },

                "org_address": {
                  "term_vector": "with_positions_offsets",
                  "store": True,
                  "type": "text",
                  #"null_value": "NULL"
                },
                "org_name": {
                  "term_vector": "with_positions_offsets",
                  "store": True,
                  "type": "text",
                  #"analyzer": "name_stopwords",
                  "fielddata": True,
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                }
        }
        }#,ignore=400
    )
    
def do_search(client,index_name,body:dict):
    """return a search result based on body content."""
    return client.search(index=index_name,body=body)

def do_match(row,client,index_name):#,results=defaultdict(list)):
    """find matched sites records, otherwiese the parent org, otherwise creating new org"""
    
    #################################
    # first step: match sites by UID
    #################################
    q1 = {"query":
          {"term":
           {"site_uid":str(row['uid'])
           }
          }
         }
    #print('q1',q1)
    m_st =  client.search(index=index_name,body=q1)#do_search(client,index_name,q1)
    #print(m_st)
    if m_st['hits']['total']['value']:
        #print('uid match')
        parent_id = m_st['hits']['hits'][0]['_source']['join_field']['parent']
        client.index(index=index_name, id=str(row['_id']),
                 body={"site_uid":str(row['uid']),"site_name": row['name'], "site_address": row['address'], "join_field": {"name": "sites", "parent":parent_id }},
                 routing=parent_id)
        #results['sucess'].append(f'uid_match with _id: {row['_id']}')
        return f"historical_uid_match with _id: {m_st['hits']['hits'][0]['_id']}",str(row['uid'])
    
    ##################################
    # second step: match sites with 
    # name and address (should parse the address before)
    ###################################
    q2 = {"query":{
            "bool": {
              "should": [
                { "match": { "site_name":row['name'] }},
                { "match": { "site_address": row['address'] }}
              ]
            }
            }
          }
    
    
#     {"query":
#           {"query_string":
#            {
#                "query": f"(site_name:{row['name']}) OR (site_address:{row['address']})"
#            }
#           }
#          }
    m_st2 = do_search(client,index_name,q2)
    if m_st2['hits']['total']['value'] and m_st2['hits']['hits'][0]['_score']>4:
        parent_id = m_st2['hits']['hits'][0]['_source']['join_field']['parent']
        client.index(index=index_name, id=str(row['_id']),
                 body={"site_uid":str(row['uid']),"site_name": row['name'], "site_address": row['address'], "join_field": {"name": "sites", "parent":parent_id }},
                 routing=parent_id)
        #results['sucess'].append(f'name_address_match with _id: {row['_id']}')
        return f"historical_name_address_match with _id: {m_st2['hits']['hits'][0]['_id']}",str(row['uid'])
    
    ####################################
    # third step: match organization with 
    # site name fuzzy match
    ####################################
    q3 = {"query":
          {"match":
           {"org_name":row['name']}}  
      }
    
    m_org = do_search(client,index_name,q3)
    if m_org['hits']['total']['value']:
        parent_id = m_org['hits']['hits'][0]['_id']
        #results['sucess'].append(f'site_org_match with _id: {row['_id']}')
        client.index(index=index_name, id=str(row['_id']),
                 body={"site_uid":str(row['uid']),"site_name": row['name'], "site_address": row['address'], "join_field": {"name": "sites", "parent":parent_id }},
                 routing=parent_id)
        return f"site_org_match with org _id: {parent_id}",str(row['uid'])
    
    ####################################
    # third four: not match
    ####################################   
    parent_id = str(row['_id'])+"1" #will change to better mothod
    client.index(index=index_name,id=parent_id,
                 body={"org_name": row['name'], "join_field": {"name": "orgs"}})
    client.index(index=index_name, id=str(row["_id"]),
                     body={"site_uid":str(row['uid']),"site_name": row['name'], "site_address": row['address'], "join_field": {"name": "sites", "parent":parent_id }},
                 routing=parent_id)
    return f"create new org-site with _id: {parent_id}",str(row['uid'])
    

    
def p2child(client,index_name,org_name):
    """search the sites of specific organization name"""
    q={
      "query": {
        "has_parent": {
          "parent_type": "orgs",
          "query": {
            "match": {
              "org_name":org_name

              }
            }
          }
        }
      }
    
    res = do_search(client,index_name,q)
    child = [f"uid:{r['_source']['site_uid']},name:{r['_source']['site_name']}" for r in res['hits']['hits']]
    return {'parent':[f"organization:{org_name}"],"child":child}
