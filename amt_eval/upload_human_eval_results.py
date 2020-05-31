import sys
import argparse
from time import gmtime, strftime

from analyze_2choice_responses import *


def arguments():
    parser = argparse.ArgumentParser(description="Arguments to upload the results into the ChatEval DB")
    parser.add_argument("--username", "-u", required=False, default='jsedoc', 
                        help="This is the username for the database [default: jsedoc]")
    parser.add_argument("--password", "-p", required=True, 
                        help="This is the password for the database")
    parser.add_argument("--hostname", required=False, default="35.237.91.101",
                        help="This is the hostname/ip of the database")
    parser.add_argument("--schema", "-s", required=False, default="demo",
                        help="This is the database schema [default: demo]")
    parser.add_argument("--evalset-id", '-e', required=True, help="Evaluation set ID")
    parser.add_argument("--model-1-id", required=True, help="model 1 ID")
    parser.add_argument("--model-2-id", required=True, help="model 2 ID")
    parser.add_argument("--path", required=True, help="path to the evaluation folder")
    parser.add_argument("--dryrun", required=False, action='store_true', help="Dryrun without DB writing")
    
    return parser.parse_args()

def connect(hostname="35.237.91.101", user="jsedoc", passwd="", db="demo"):
    try:
        import mysql.connector
        db = mysql.connector.connect(host=hostname, user=user, passwd=passwd, db=db)
    except:
        import MySQLdb
        db=MySQLdb.connect(host=hostname, user=user, passwd=passwd, db=db)
    c=db.cursor()
    return (c, db)

def get_last_ids(db_connector):
    c = db_connector
    c.execute('SELECT max(mturk_run_id_id), max(id) FROM demo.HumanEvaluationsABComparison')
    (last_mturk_id, last_eval_id) = c.fetchone()
    return (last_mturk_id, last_eval_id)

def get_eval_min_prompt(db_connector, evalset_id):
    c = db_connector
    c.execute('SELECT min(prompt_id) FROM demo.EvaluationDatasetText WHERE evaluationdataset_id=' + str(evalset_id))
    min_prompt_id = c.fetchone()[0]
    return min_prompt_id

def check_duplicate(db_connector, evalset_id, m1id, m2id):
    c = db_connector
    c.execute('SELECT * FROM demo.HumanEvaluations where evaluationdataset_id=' + str(evalset_id) + ' and model_1=' + str(m1id) + ' and  model_2=' + str(m2id))
    if len(c.fetchall())>0:
        return True
    return False
        
def upload_evaluation(evalset_id, m1id, m2id, path, mturk_run_id, eval_id, min_prompt_id):
    insert_into_humanevals_table=True

    target_files = open(path + '/order.txt').readlines()
    target_files[0] =  target_files[0].strip()
    target_files[1] =  target_files[1].strip()

    examples = utils.process_source_and_responses(
        os.path.abspath(os.path.join('../eval_data/ncm/neural_conv_model_eval_source.txt')), target_files)

    examples_dict = {}
    for example in examples:
        examples_dict[example.key] = example

    worker_results_list = pickle.load(open(path + '/amt_hit_responses.pkl','rb'))
    for i,r in enumerate(worker_results_list):
        try:
            subdt = r['Assignments'][0]['AcceptTime']
        except:
            #import pdb; pdb.set_trace()
            pass
        
        
    utils.process_amt_hit_responses(worker_results_list, examples_dict)

    for (key, ex) in examples_dict.items():
        #print(ex.hits)
        #import pdb; pdb.set_trace()
        #for worker, vote, hit, accept_dt in zip(ex.workers, ex.votes, ex.hits, ex.acceptdates):
        for worker, vote, hit in zip(ex.workers, ex.votes, ex.hits):
            #print(worker + '\t' +  m1.replace(' ','_')+'-'+m2.replace(' ','_')+'-'+key + '\t' +  str(vote))
            # HACK ---
            dt = subdt
            accept_dt = subdt
            dt = accept_dt.strftime("%Y-%m-%d %H:%M:%S")
            if insert_into_humanevals_table == True:
                print("""INSERT INTO HumanEvaluations (Mturk_run_id, Submit_datetime, Results_path, Evaluationdataset_id, Model_1, Model_2) VALUES (%s, %s, %s, %s, %s, %s)""" , (mturk_run_id,dt,path,evalset_id,m1id,m2id))
                c.execute("""INSERT INTO HumanEvaluations (Mturk_run_id, Submit_datetime, Results_path, Evaluationdataset_id, Model_1, Model_2) VALUES (%s, %s, %s, %s, %s, %s)""" , (mturk_run_id,dt,path,evalset_id,m1id,m2id))
                insert_into_humanevals_table = False

            print(eval_id, worker, hit, accept_dt, mturk_run_id, str(int(key.strip('ex-')) + 1))

            
            print("""INSERT INTO `demo`.`HumanEvaluationsABComparison` (`id`, `worker_id`, `hit`, `accept_datetime`, `value`, `mturk_run_id_id`, `prompt_id`) VALUES (%s, %s, %s, %s, %s, %s, %s)""", (eval_id, worker, hit, dt, vote, mturk_run_id, str(int(key.strip('ex-')) + min_prompt_id)))
            c.execute("""INSERT INTO `demo`.`HumanEvaluationsABComparison` (`id`, `worker_id`, `hit`, `accept_datetime`, `value`, `mturk_run_id_id`, `prompt_id`) VALUES (%s, %s, %s, %s, %s, %s, %s)""", (eval_id, worker, hit, dt, vote, mturk_run_id, str(int(key.strip('ex-')) + min_prompt_id)))
            
            eval_id += 1
        #import pdb; pdb.set_trace()
    mturk_run_id += 1

if __name__ == "__main__":
    args = arguments()

    (c, db) = connect(passwd=args.password)
    (last_mturk_id, last_eval_id) = get_last_ids(c)

    min_prompt_id = get_eval_min_prompt(c, args.evalset_id)
    
    if not check_duplicate(c, args.evalset_id, args.model_1_id, args.model_2_id):
        upload_evaluation(args.evalset_id, args.model_1_id, args.model_2_id, args.path, last_mturk_id+1, last_eval_id+1, min_prompt_id)
        if not args.dryrun:
            print("committing to DB")
            db.commit()
    else:
        print("This may be duplicate ... please check")
