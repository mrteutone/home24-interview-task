# Item Recommender
home24 take home task for Data Engineers

Philipp Marx, phlppmarx@gmail.com

See `task-description.md` for more details about the task.

## How to run
1. Unpack the archive
2. Open a terminal and navigate to the unpacked project's root directory.
3. [*optional*] Run `sbt test` to see if everything works correctly.
4. Run (replace `sku-123` with your own choice):
   
   `sbt "run --sku sku-123"`
   
   This will print 10 recommended items at the end of the screen.
   The recommendations are based on `test-data-for-spark.json` in the project's root directory.
   See the notes below for more options.
   
**Notes**

Besides a SKU you can specify the number of recommendations
and the input file containing items used as recommendations.
See the example below:
```
sbt "run
  --sku sku-123
  --numberOfRecommendations 10
  --itemsPath <your-file.json> "
```

## How does it work
The program has the following work-flow:

1. Read the specified or default *json* file containing the items by using Spark's `DataFrame` API.
2. Extract the attributes of the SKU given by the user from the dataframe containing the items.
3. Within each row of the dataframe containing the items:
   * Calculate the matching attributes and count them as *matching count*
   * Sort the matching attributes alphanumerically
4. Calculate until which *matching count* all items should be selected as recommendations.
5. Calculate how many next best additional items should be selected from the next best *matching count*
   so that together with 4. the required number of recommendations is reached.
6. Select the items according to 4. and 5. from the dataframe containing all items and print them on the screen.

Note that by using the logic described in 4. and 5. we avoid sorting the whole dataframe.
This is especially inefficient if we are only interested in the top-*n* rows for small *n*.

## Design Choices
The given *item-to-item recommendation* task can be categorized as *content-based filtering*.
Furthermore, the requirements describe a version of *nearest neighbor search*.
The best known algorithm for this is *k-nearest neighbors* (*k*-NN).

Unfortunately, there is no implementation of **exact** k-NN available in Spark's ML library *mllib*.
The main reason is that *k*-NN is a so called lazy learner, i. e. in this case the
training data itself is the model and work only starts if a SKU is specified by the user.

That said I opted for a custom implementation to match the custom similarity metric described in the requirements.
However, in reality one would probably use a more efficient *approximate k-NN* algorithm available in *mllib* (see [1]).

[1]: https://spark.apache.org/docs/latest/ml-features.html#approximate-nearest-neighbor-search

