// Binds the traversal source g over the graph defined in conf/aerolpg.properties
def globals = [:]
globals << [g : traversal().withEmbedded(graph)]
