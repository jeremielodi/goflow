            +------------------+
            |   main.go        |
            +--------+---------+
                     |
                     v
            +------------------+
            | Engine           |
            | - definitions    |
            | - instances      |
            | - jobs           |
            +--------+---------+
                     |
     +---------------+----------------+
     |                                |
     v                                v
Process Instance              Job Worker Pool
(State Machine)               (Service Tasks)