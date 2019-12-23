db.movies.aggregate([
    {
        $match: {
            awards: {
                $regex: /^Won \d+ Oscars?/
            }
        }
    },
    {
        $group: {
            "_id": null,
            "highest_rating": {
                $max: "$imdb.rating"
            },
            "lowest_rating": {
                $min: "$imdb.rating"
            },
            "average_rating": {
                $avg: "$imdb.rating"
            },
            "deviation": {
                $stdDevSamp: "$imdb.rating"
            },
        }
    },
])


db.movies.aggregate([
    {
        $match: {
            "languages": "English",
        }
    },
    {
        $unwind: "$cast"
    },
    {
        $group: {
            _id: "$cast",
            numFilms: { $sum: 1 },
            average: { $avg: "$imdb.rating" }
        }
    },
    {
        $sort: {
            numFilms: -1,
        }
    }
])

db.movies.aggregate([
    {
        $match: {
            languages: "English"
        }
    },
    {
        $project: { _id: 0, cast: 1, "imdb.rating": 1 }
    },
    {
        $unwind: "$cast"
    },
    {
        $group: {
            _id: "$cast",
            numFilms: { $sum: 1 },
            average: { $avg: "$imdb.rating" }
        }
    },
    {
        $project: {
            numFilms: 1,
            average: {
                $divide: [{ $trunc: { $multiply: ["$average", 10] } }, 10]
            }
        }
    },
    {
        $sort: { numFilms: -1 }
    },
    {
        $limit: 1
    }
])


db.air_routes.aggregate([
    {
        $match: {
            "airplane": { $regex: /747|380/ }
        }
    },
    {
        $lookup: {
            from: "air_alliances",
            localField: "airline.name",
            foreignField: "airlines",
            as: "info_airline",
        }
    }
]).pretty()

db.air_routes.aggregate([
    {
        $match: {
            airplane: /747|380/
        }
    },
    {
        $lookup: {
            from: "air_alliances",
            foreignField: "airlines",
            localField: "airline.name",
            as: "alliance"
        }
    },
    {
        $unwind: "$alliance"
    },
    {
        $group: {
            _id: "$alliance.name",
            count: { $sum: 1 }
        }
    },
    {
        $sort: { count: -1 }
    }
])

db.air_alliances.aggregate([
    {
        $match: { name: "OneWorld" }
    },
    {
        $graphLookup: {
            startWith: "$airlines",
            from: "air_airlines",
            connectFromField: "name",
            connectToField: "name",
            as: "airlines",
            maxDepth: 0,
            restrictSearchWithMatch: {
                country: { $in: ["Germany", "Spain", "Canada"] }
            }
        }
    },
    {
        $graphLookup: {
            startWith: "$airlines.base",
            from: "air_routes",
            connectFromField: "dst_airport",
            connectToField: "src_airport",
            as: "connections",
            maxDepth: 1
        }
    },
    {
        $project: {
            validAirlines: "$airlines.name",
            "connections.dst_airport": 1,
            "connections.airline.name": 1
        }
    },
    { $unwind: "$connections" },
    {
        $project: {
            isValid: {
                $in: ["$connections.airline.name", "$validAirlines"]
            },
            "connections.dst_airport": 1
        }
    },
    { $match: { isValid: true } },
    {
        $group: {
            _id: "$connections.dst_airport"
        }
    }
])
