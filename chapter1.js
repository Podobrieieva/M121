var pipeline = [
    {
        $match: {
            "imdb.rating": { $gte: 7 },
            "genres": { $nin: ["Horror", "Crime"] },
            "rated": { $exists: true, $regex: /^P?G$/ },
            "languages": { $all: ["English", "Japanese"] }
        }
    },
    {
        $project: {
            _id: 0,
            title: 1,
            rated: 1
        }
    }
]

var pipeline = [
    {
        $project: {
            "title": 1,
            "title_size": { $size: { $split: ["$title", " "] } },
        }
    },
    {
        $match: { "title_size": 1 }
    },
]


db.movies.aggregate([
    {
        $match: {
            cast: { $elemMatch: { $exists: true } },
            directors: { $elemMatch: { $exists: true } },
            writers: { $elemMatch: { $exists: true } }
        }
    },
    {
        $project: {
            _id: 0,
            cast: 1,
            directors: 1,
            writers: {
                $map: {
                    input: "$writers",
                    as: "writer",
                    in: {
                        $arrayElemAt: [
                            {
                                $split: ["$$writer", " ("]
                            },
                            0
                        ]
                    }
                }
            }
        }
    },
    {
        $project: {
            labor_of_love: {
                $gt: [
                    { $size: { $setIntersection: ["$cast", "$directors", "$writers"] } },
                    0
                ]
            }
        }
    },
    {
        $match: { labor_of_love: true }
    },
    {
        $count: "labors of love"
    }
])



