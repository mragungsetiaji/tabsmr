
import pandas as pd
import psycopg2
import datalab.storage as gcs

class TabsMR(object):

    def __init__(self):
        self.rawdata = None
        self.access_granted = False # digunakan untuk verify api
        self.access_message = None
        self.bucket_name = None

        self.registered_rawdata = False

    def credentials(self, certificate):
        """
        certificate = {
            "account_type": "tabulation",
            "client":"clientname",
            "project_id": "project_id",
            "private_key_id": "a4a9f3c7600081ea9bad46ece1b158e2f16454e2",
            "user_email": "blablabla@gmail.com",
            "user_id": "101670599119528512817",
        }

        """
        self.certificate = certificate
	
        conn = psycopg2.connect(
            host='localhost',
            port=54320,
            dbname='my_database',
            user='postgres',
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT private_key_id FROM public.credentials
            WHERE account_type={0}
                AND project_id={0}
                AND user_email={0}
                AND user_id={0};""".format(
                    certificate["account_type"], certificate["project_id"],
                    certificate["user_email"], certificate["user_id"]
                ))
        rec = cur.fectone()

        if rec["private_key_id"] == certificate["private_key_id"]:
            self.access_granted = True
            self.access_message = "Your access credentials is granted!"
        else:
            self.access_message = "Your access credentials is denied, Please recheck your key!"

    def do_register_rawdata(self, csv_path):
        """
        register rawdata ke google storage bucket
        """
        if self.registered_rawdata == False:
    ¸       a = pd.read_csv(csv_path)
            gcs.Bucket(self.bucket_name).item(certificate["clientname"]+"/dataset_"+certificate["project_id"] +".csv")\
                                        .write_to(a.to_csv(),'text/csv')
            self.registered_rawdata == True
            print("Your rawdata is already registered")
        else:
            print("Your rawdata is already registered")
        
        return None

    def register_spec(self, spec)
        return None


    def exec(self, **params):
        pass

class TabsUtils(object):

    def __init__(self):
        pass
    
    def do_weighting(self, rawdata, weight_formula):
    
        """
        data: dataframe crosstab
            hasil dari crosstab
        weight_formula: dict
            {column_name: weight_score}
            
        """
        a = rawdata
        for index in weight_formula:
            a[index + "_weighted"] = a[index] * weight_formula[index]
            a[index + "_weighted"] = a[index + "_weighted"].round(0).astype(int)
            
        return a

    def do_sure_header_column(self, concat_tbl, label):
        """
        # all column header harus tersedia, meskipun hasilnya na
        """
        if label in concat_tbl.columns:
            pass
        else:
            concat_tbl[label] = 0
        return concat_tbl


class TabsQuestion(TabsMR):

    def __init__(self):
        pass

    def single(self, data, column_for_index, column_for_header, spec,
               merge_for_header=None, display_ex_merge=None,
               percentage=None):
        
        """
        data: dataframe
            rawdata
        column_for_index: int
            kolom pada data yang mau dijadiin index pada crosstab
        column_for_header: array int
            kolom pada data yang mau dijadiin header pada crosstab
        merge_for_header: dict
            {"16-25 tahun":[2,3],
            "25-40 tahun":[4,5,6],
            "46-55 tahun":[7,8,9]}
            -- will be explain later
        display_ex_merge: Booelan
            nampilin yang sebelum di merge atau ga
        """
        column_header_label = spec
        for i in range(len(column_for_header)):
            if i == 0:
                a = pd.crosstab(data[data.columns[column_for_index]], 
                                data[data.columns[column_for_header[i]]],
                                dropna=False)
            else:
                a = pd.concat([a, 
                               pd.crosstab(data[data.columns[column_for_index]], 
                                           data[data.columns[column_for_header[i]]],
                                           dropna=False)
                              ], axis=1)
        
        # all column header harus tersedia, meskipun hasilnya na
        pu = PandaUtils()
        for i in column_header_label:
            a = pu.do_sure_header_column(a, i)
        
        a = a[spec]
        
        if merge_for_header != None:
            all_collections = []
            for index in merge_for_header:
                if len(merge_for_header[index]) != 1:
                    a[index] = 0
                    for collections in merge_for_header[index]:
                        a[index] = a[index] + a[a.columns[collections]]
                        all_collections.append(collections)
        
        if display_ex_merge != None:
            display_columns= []
            for i in range(len(a.columns)):
                if i in all_collections:
                    pass
                else:
                    display_columns.append(a.columns[i])
            a = a[display_columns]
            
        a.index.name = a.index.name[:3]
        
        if percentage != None:
            return a.apply(lambda r: r/r.sum(), axis=1)
        else:
            return a

    def multi(self, data, column_for_index, column_for_header, spec,
                    merge_for_header=None, 
                    display_ex_merge=None,
                    percentage=None):
        
        # looking for list label for all column header
        column_header_label = spec
        
        for j in range(len(column_for_header)):
            
            if j == 0:
                
                for i in range(len(column_for_index)):
                    if i == 0:
                        a = pd.crosstab(data[data.columns[column_for_index[i]]], 
                                        data[data.columns[column_for_header[j]]],
                                        dropna=False)
                        a.index = [data.columns[column_for_index[i]] + " - " + k for k in a.index]

                    else:
                        b = pd.crosstab(data[data.columns[column_for_index[i]]], 
                                        data[data.columns[column_for_header[j]]],
                                        dropna=False)
                        b.index = [data.columns[column_for_index[i]] + " - " + k for k in b.index]

                        a = pd.concat([a, b])
                c = a
            else:
                                
                for i in range(len(column_for_index)):
                    if i == 0:
                        a = pd.crosstab(data[data.columns[column_for_index[i]]], 
                                        data[data.columns[column_for_header[j]]],
                                        dropna=False)
                        a.index = [data.columns[column_for_index[i]] + " - " + k for k in a.index]

                    else:
                        b = pd.crosstab(data[data.columns[column_for_index[i]]], 
                                        data[data.columns[column_for_header[j]]],
                                        dropna=False)
                        b.index = [data.columns[column_for_index[i]] + " - " + k for k in b.index]

                        a = pd.concat([a, b])
                
                c = pd.concat([a, c], axis=1)
        
        a = c

        # all column header harus tersedia, meskipun hasilnya na
        pu = PandaUtils()
        for i in column_header_label:
            a = pu.do_sure_header_column(a, i)
        
        a = a[spec]
        if merge_for_header != None:
            all_collections = []
            for index in merge_for_header:
                if len(merge_for_header[index]) != 1:
                    a[index] = 0
                    for collections in merge_for_header[index]:
                        a[index] = a[index] + a[a.columns[collections]]
                        all_collections.append(collections)
        
        if display_ex_merge != None:
            display_columns= []
            for i in range(len(a.columns)):
                if i in all_collections:
                    pass
                else:
                    display_columns.append(a.columns[i])
            a = a[display_columns]
            
        #a.index.name = a.index.name[:3]
        
        if percentage != None:
            return a.apply(lambda r: r/r.sum(), axis=1)
        else:
            return a

    def rank(self, data, column_for_index):
        
        for i in range(len(column_for_index)):
            if i == 0:
                a = pd.DataFrame(data[data.columns[column_for_index[i]]].value_counts())
            else:
                b = pd.DataFrame(data[data.columns[column_for_index[i]]].value_counts())
                a = pd.concat([a,b], axis=1)
        
        b = a.T
        c = b.apply(lambda r: r/r.sum(), axis=1)
        c.columns = [i + " (%)" for i in c.columns]
        return pd.concat([b,c], axis=1)

