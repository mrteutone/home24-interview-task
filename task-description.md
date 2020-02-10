### Input Data
The given JSON file contains 20K articles (1 object per line).
Each article contains set of attributes with one set value.

### Recommendation Engine Requirements
Calculate similarity of articles identified by SKU (product identifier) based on their attributes values.
The number of matching attributes is the most important metric for defining similarity.
In case of a draw, attributes with a name higher in alphabet (a is higher than z) is weighted heavier.

Example 1:
{"sku":"sku-1","attributes": {"att-a": "a1", "att-b": "b1", "att-c": "c1"}} is more similar to
{"sku":"sku-2","attributes": {"att-a": "a2", "att-b": "b1", "att-c": "c1"}} than to
{"sku":"sku-3","attributes": {"att-a": "a1", "att-b": "b3", "att-c": "c3"}}

Example 2:
{"sku":"sku-1","attributes":{"att-a": "a1", "att-b": "b1"}} is more similar to 
{"sku":"sku-2","attributes":{"att-a": "a1", "att-b": "b2"}} than to
{"sku":"sku-3","attributes":{"att-a": "a2", "att-b": "b1"}}

### Recommendation request example
sku-123 > ENGINE > 10 most similar SKUs based on criteria described above with their corresponding weights.

### Implementation requirements
Please use Spark with Scala for your solution.
You can use any of Spark's APIs and modules like RDDs, Dataframes, SQL, etc. 
You can use sbt to manage Spark libs.
The Spark Application/session should be set up to run on a local machine.

### Code requirements
Clean structured reusable code.

### Expected delivery format
Gzipped file containing solution with simple instructions how to run data import and how to execute recommendation request.
