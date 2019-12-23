db.movies.aggregate([
    {
        $match: {
            "metacritic": {
                $exists: true,
                $ne: null
            }
        }
    },
    {
        $facet: {
            "Rating": [
                {
                    $bucket: {
                        groupBy: '$imdb.rating',
                        boundaries: [0, 8.7, Infinity],
                        output: {
                            total: { $sum: 1 },
                            ids: { $addToSet: '$_id' }
                        }
                    }
                }
            ],
            "Metacritic": [
                {
                    $bucket: {
                        groupBy: '$metacritic',
                        boundaries: [0, 100, Infinity],
                        output: {
                            total: { $sum: 1 },
                            ids: { $addToSet: '$_id' }
                        }
                    }
                }
            ]
        }
    },
    {
        $project: {
            "rating": { $arrayElemAt: ["$Rating", 1] },
            "meta": { $arrayElemAt: ["$Metacritic", 1] }
        }
    }, {
        $project: {
            "r": { $size: "$rating.ids" },
            m: { $size: "$meta.ids" },
            "amount": {
                $size: {
                    $setIntersection: ["$rating.ids", "$meta.ids"]
                }
            }
        }
    }
]).pretty()

db.movies.aggregate([
    {
        $match: {
            "metacritic": {
                $exists: true,
                $gte: 9
            }
        }
    }, {
        $group: {
            _id: "$imdb.rating",
            count: { $sum: 1 },
            max: {
                $max: "$imdb.rating"
            }
        }
    }
]).pretty()

db.movies.aggregate([
    {
        $match: {
            metacritic: { $gte: 0 },
            "imdb.rating": { $gte: 0 }
        }
    },
    {
        $project: {
            _id: 0,
            metacritic: 1,
            imdb: 1,
            title: 1
        }
    },
    {
        $facet: {
            top_metacritic: [
                {
                    $sort: {
                        metacritic: -1,
                        title: 1
                    }
                },
                {
                    $limit: 10
                },
                {
                    $project: {
                        title: 1
                    }
                }
            ],
            top_imdb: [
                {
                    $sort: {
                        "imdb.rating": -1,
                        title: 1
                    }
                },
                {
                    $limit: 10
                },
                {
                    $project: {
                        title: 1
                    }
                }
            ]
        }
    },
    {
        $project: {
            movies_in_both: {
                $setIntersection: ["$top_metacritic", "$top_imdb"]
            }
        }
    }
])


