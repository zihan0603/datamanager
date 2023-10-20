import pymongo
import unittest
import json
import logging
"""
z
This is a reST style.

:param param_1: this is the first parameter
:param param_2: this is the second parameter
:returns: this is a description of what is returned
:raises keyError: raises an exception

"""

# use a global variable to store your collection object
mycol = None


"""
Connect to the local MongoDB server, database, and collection.

"""
def initialize():
    # specify that we are using the global variable mycol
    global mycol
    
    #########################
    # INSERT YOU CODE BELOW #
    #########################

    myclient = pymongo.MongoClient("mongodb+srv://Cluster47615:passwordpassword@cluster0.kyo2wxs.mongodb.net/?retryWrites=true&w=majority")
    mydb = myclient["mydatabase"]
    mycol = mydb["collection"]


"""
Drop the collection and reset global variable mycol to None.

"""
def reset():
    # specify that we are using the global variable mycol
    global mycol

    #########################
    # INSERT YOU CODE BELOW #
    #########################
    mycol.drop()
    mycol = None


"""
Insert document(s) into the collection.

:param document: a Python list of the document(s) to insert
:returns: result of the operation

"""
def create(document):
    #########################
    # INSERT YOU CODE BELOW #
    #########################
    list_doc = []
    if type(document) == dict:
        list_doc.append(document)
    elif (type(document) != list):
        return False
    else:
        list_doc = document
    mycol.insert_many(list_doc)
    return True


"""
Retrieve document(s) from the collection that match the query,
if parameter one is True, retrieve the first matched document,
else retrieve all matched documents.

:param query: a Python dictionary for the query
:param one: an indicator of how many matched documents to be retrieved, by default its value is False
:returns: all matched document(s)

"""
def read(query, one=False):
    #########################
    # INSERT YOU CODE BELOW #
    #########################
    
    if one:
        return mycol.find_one(query)
    else:
        logging.info(type(query))
        if query == str:
            query = json.loads(query)
        return_list = []
        find = mycol.find(query)
        for i in find:
            return_list.append(i)
        return return_list

"""
Update document(s) that match the query in the collection with new values.

:param query: a Python dictionary for the query
:param new_values: a Python dictionary with updated data fields / values
:returns: result of the operation

"""
def update(query, new_values):
    #########################
    # INSERT YOU CODE BELOW #
    #########################
    if query != dict:
        return False
    if new_values != dict:
        return False
    mycol.update_many(query, {"$set": new_values})
    return True


"""
Remove document(s) from the collection that match the query.

:param query: a Python dictionary for the query
:returns: result of the operation

"""
def delete(query):
    #########################
    # INSERT YOU CODE BELOW #
    #########################
    if query != dict:
        return False
    mycol.delete_many(query)
    return True

class unitTest(unittest.TestCase):
    def test_initialize(self):
        initialize()
        self.assertNotEqual(None, mycol)
    
    def test_create(self):
        initialize()
        documents = [{"_id": 1, "title": "test1", "msg": "test1"}, 
                     {"_id": 2, "title": "test2", "msg": "test2"}, 
                     {"_id": 3, "title": "test3", "msg": "test3"}]
        self.assertEqual([1, 2, 3], create(documents).inserted_ids)
        mycol.drop()
    
    def test_read(self):
        initialize()
        documents = [{"_id": 1, "title": "test1", "msg": "test1"}, 
                     {"_id": 2, "title": "test2", "msg": "test2"}, 
                     {"_id": 3, "title": "test3", "msg": "test3"},
                     {"_id": 4, "title": "test1", "msg": "duplicate"}]
        create(documents)

        self.assertEqual({'_id': 1, 'title': 'test1', 'msg': 'test1'}, read({"title": "test1"}, True))
        multiple = read({"title": "test1"})
        self.assertEqual({'_id': 1, 'title': 'test1', 'msg': 'test1'}, multiple[0])
        self.assertEqual({'_id': 4, 'title': 'test1', 'msg': 'duplicate'}, multiple[1])
        mycol.drop()
    
    def test_reset(self):
        initialize()
        self.assertNotEqual(None, mycol)
        reset()
        self.assertEqual(None, mycol)
    
    def test_update(self):
        initialize()
        documents = [{"_id": 1, "title": "test1", "msg": "test1"}]
        create(documents)
        update({"_id": 1}, {"msg": "UPDATED"})
        self.assertEqual({"_id": 1, "title": "test1", "msg": "UPDATED"}, read({"_id": 1}, True))
        reset()

    def test_delete(self):
        initialize()
        documents = [{"_id": 1, "title": "test1", "msg": "test1"}]
        create(documents)
        self.assertEqual(1, delete({"_id": 1}).deleted_count)
        reset()
        

if __name__ == "__main__":
    # sample tests
    initialize()
    # x = create({'title':'test','message':"testing 1 2"})
    doc = read({}, one=False)
    print(doc)

    # update({'title':'test'}, {'message':"testing 3 4"})
    # doc = read({}, one=True)
    # print(doc)

    # delete({'title':'test'})
    # doc = read({}, one=True)
    # print(doc)

    # reset()

    #########################
    # INSERT YOU TESTS BELOW #
    #########################

    # unit tests at line 110
    # unittest.main()