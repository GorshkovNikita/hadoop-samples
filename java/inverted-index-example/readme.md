# Variant 3. Inverted Index

## Sample Input File

    D[0]=This is sample input file
    D[1]=Each line presents separate document
    D[2]=Part before the equal sign is the name of the document
    D[3]=Part after the equal sign is the text of document

## Output from the MapReduce Task

    Each	(D[1] 0)
    Part	(D[3] 0) (D[3] 0)
    This	(D[0] 0)
    after	(D[3] 5)
    before	(D[2] 5)
    document	(D[2] 46) (D[2] 46) (D[2] 46)
    equal	(D[3] 15) (D[3] 15)
    file	(D[0] 21)
    input	(D[0] 15)
    is	(D[0] 5) (D[0] 5) (D[0] 5)
    line	(D[1] 5)
    name	(D[2] 34)
    of	(D[2] 39) (D[2] 39)
    presents	(D[1] 10)
    sample	(D[0] 8)
    separate	(D[1] 19)
    sign	(D[3] 21) (D[3] 21)
    text	(D[3] 33)
    the	(D[3] 29) (D[3] 29) (D[3] 29) (D[3] 29) (D[3] 29)


   
  
