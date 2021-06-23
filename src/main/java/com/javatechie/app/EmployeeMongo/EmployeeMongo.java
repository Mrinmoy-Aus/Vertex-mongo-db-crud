package com.javatechie.app.EmployeeMongo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.bson.Document;

import com.google.gson.Gson;
import com.javatechie.app.EmployeeMongo.Employee;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;


public class EmployeeMongo {
  public static void main(String[] args) {
    System.out.println("Hello World!");
    List<Employee> employees = new ArrayList<>();
    
    String client_url = "mongodb://"+"localhost"+":"+27017; // connection string
    
    MongoClientURI mongoClientURI = new MongoClientURI(client_url); //mongoClientURI reference
    MongoClient mongoClient = new MongoClient(mongoClientURI);//mongoClient reference
    
    MongoDatabase db = mongoClient.getDatabase("EmployeeDB"); // database reference
    
    Vertx vertx = Vertx.vertx(); // vertx object
    HttpServer server = vertx.createHttpServer(); // httpServer object
    
    Router router = Router.router(vertx); //router object
	  
    vertx.eventBus().consumer("hello.vertx.addr", msg->{
            String name = (String)msg.body();
            MongoCollection<Document> collection = db.getCollection("employeeInfo");//collection reference
            MongoCursor<Document> cur = collection.find().iterator();//cursor pointing to the current document
            List<String> li = new ArrayList<>();//creating a list
            while(cur.hasNext()) {
                Document docu  = cur.next();//pointing to the current document and then moving to the next
                Collection<Object> info = docu.values();//getting the value of the document
                String json = new Gson().toJson(info );//converting to string
                if(json.contains(name)) {//matching whether it contains the name
                    li.add(json);
                }

            }
            msg.reply(li.toString());
        });
    
    /**
     * @POST Method
     */
    
    Route postHandler = router.post("/addInMongo").handler(BodyHandler.create()).handler(routingContext -> {
		final Employee employee = Json.decodeValue(routingContext.getBody(), Employee.class);
		HttpServerResponse serverResponse = routingContext.response(); //server response Object
		serverResponse.setChunked(true); // marking the response
		    Document doc = new Document("id", employee.getId())// making the document
		    .append("name", employee.getName())
		    .append("dept", employee.getDept())
		    .append("salary", employee.getSalary());

		    db.getCollection("employeeInfo").insertOne(doc);//calling the collection and inserting the document
//		employees.add(employee);
		
		serverResponse.end(" Employee added successfully..."); // giving the response to the caller
	});
    
    /**
     * @GET Method
     */
    Route getHandler = router.get("/getFromMongo").produces("*/json").handler(routingContext->{
    	
        MongoCollection<Document> collection = db.getCollection("employeeInfo"); // collection reference
        MongoCursor<Document> cur = collection.find().iterator(); // cursor pointing to the first document
        List<String> li = new ArrayList<>(); // making a list to store strings
        while(cur.hasNext()) {
        	Document docu  = cur.next();//pointing to the current document and then moves to the next
        	Collection<Object> info = docu.values();//getting the value of the document
        	String json = new Gson().toJson(info);//converting to string
        	li.add(json);//adding to the list
        }
        routingContext.response().setChunked(true).end(li.toString());//returning the list as a answer to the caller
    	
    });
    
    /**
	 * @GET Method with Filter
	 *
	 */
	Route getFilterHandler = router.get("/getFromMongo/:name").produces("*/json").handler(routingContext -> {
		String name = routingContext.request().getParam("name"); // extracting the name from the url
		
       		 vertx.eventBus().send("hello.vertx.addr",name,reply->{
                routingContext.request().response().end((String)reply.result().body());
            });
	});
	
	/*delete api*/
	
	Route getDeleteHandler = router.delete("/deleteFromMongo/:name").handler(routingContext -> {
		String name = routingContext.request().getParam("name"); // extracting the name from the url
		
        MongoCollection<Document> collection = db.getCollection("employeeInfo");//collection reference
        collection.deleteOne(Filters.eq("name",name)); // filtering out the name and deleting it from the collection
		
		routingContext.response().setChunked(true).end("deleted successfully");
	});
	
	/*update api*/
	
	Route getUpdateHandler = router.put("/updateFromMongo/:name/:type/:update").handler(routingContext -> {
		String name = routingContext.request().getParam("name"); // extracting the name from the url
		String type = routingContext.request().getParam("type"); // extracting the type to be updated from the url
		String update = routingContext.request().getParam("update");//updating value from the url
		
		
        MongoCollection<Document> collection = db.getCollection("employeeInfo");//getting the collection reference
        collection.updateOne(Filters.eq("name", name), Updates.set(type, update));//updating it from the collection
		
		routingContext.response().setChunked(true).end("updated successfully");
	});
	 
	   /*increment income*/

        Route getIncrementHandler = router.put("/incrementIncome/:name").handler(routingContext ->{
           String name = routingContext.request().getParam("name");
           MongoCollection<Document> collection = db.getCollection("employeeInfo");

            FindIterable<Document> somebody = collection.find(Filters.eq("name", name));

            for(Document id:somebody){
                int salary = (int) id.get("salary");
                int sal = salary+10000;
                collection.updateOne(Filters.eq("name", name), Updates.set("salary", sal));
            }


            routingContext.response().setChunked(true).end("updated successfully");


        });
	

	server.requestHandler(router::accept).listen(8080);
  }
}
