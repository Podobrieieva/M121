var favorites = [
    "Sandra Bullock",
    "Tom Hanks",
    "Julia Roberts",
    "Kevin Spacey",
    "George Clooney"]

db.movies.aggregate([
    {
        $match: {
            cast: { $elemMatch: { $exists: true } },
            countries: "USA",
            "tomatoes.viewer.rating": { $gte: 3 }
        }
    },
    {
        $project: {
            "tomatoes.viewer.rating": 1,
            title: 1,
            num_favs: {
                $size: { $setIntersection: ["$cast", favorites] },
            },
        },
    },
    {
        $sort: {
            "num_favs": -1,
            "tomatoes.viewer.rating": -1,
            "title": -1
        }
    }, {
        $skip: 24,
    }
]).pretty()

var x_max = 1521105
var x_min = 5
var min = 1
var max = 10

db.movies.aggregate([
    {
        $match: {
            "languages": "English",
            "imdb.rating": { $gte: 1 },
            "imdb.votes": { $gte: 1 },
            "released": { "$gte": new Date(1990) }
        }
    },
    {
        $project: {
            "imdb.rating": 1,
            "imdb.votes": 1,
            title: 1,
            scaled_votes: 1 + 9 * (("$imdb.votes" - x_min) / (x_max - x_min)),
        },
    },
    {
        $project: {
            "title": 1,
            "scaled_votes": 1,
            normalized_rating: {
                $avg: ["$scaled_votes", "$imdb.rating"]
            },

        },
    }, {
        $sort: {
            normalized_rating: 1
        }
    }
]).pretty()

db.movies.aggregate([
    {
        $match: {
            year: { $gte: 1990 },
            languages: { $in: ["English"] },
            "imdb.votes": { $gte: 1 },
            "imdb.rating": { $gte: 1 }
        }
    },
    {
        $project: {
            _id: 0,
            title: 1,
            "imdb.rating": 1,
            "imdb.votes": 1,
            normalized_rating: {
                $avg: [
                    "$imdb.rating",
                    {
                        $add: [
                            1,
                            {
                                $multiply: [
                                    9,
                                    {
                                        $divide: [
                                            { $subtract: ["$imdb.votes", 5] },
                                            { $subtract: [1521105, 5] }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        }
    },
    { $sort: { normalized_rating: 1 } },
    { $limit: 1 }
])