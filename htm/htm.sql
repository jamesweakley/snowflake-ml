create or replace function HTM(INPUTS array)
    returns table (ACTIVE array,PREDICTIVE array)
    language javascript
    AS '{
    
    /**
     * The HTMController contains high-level HTM functions.
     * 
     */
    HTMController: function() {
        var my = this; // Reference to self, for use in functions
        const PROXIMAL   = 0;
        const DISTAL     = 1;
        const APICAL     = 2;
        const TM_LAYER   = 0; // Receives distal input from own cells
        const TP_LAYER   = 1; // Produces stable representations

        this.layers = []; // Each layer created is stored here for easy lookup
        
        function Cell( matrix, index, x, y, column ) {

            this.matrix = matrix; // Reference to matrix containing the cell
            this.index = index;  // 1 dimentional index of the cell
            this.x = ( ( typeof x === "undefined" ) ? index : x );  // 2 dimentional x index of the cell
            this.y = ( ( typeof y === "undefined" ) ? 0 : y );  // 2 dimentional y index of the cell
            this.column = ( ( typeof column === "undefined" ) ? null : column );  // column the cell is in

            this.axonSynapses = [];  // Outputs
            this.proximalSegments = [];  // Feed-forward input
            this.distalSegments = [];
            this.apicalSegments = [];

            this.distalLearnSegment = null;  // Distal segment to train
            this.apicalLearnSegment = null;  // Apical segment to train

            this.active = false;
            this.predictedActive = false; // cell was correctly predicted
            this.predictive = false;
            this.learning = false;

            // Add this cell to its matrix
            this.matrix.cells.push( this );
        }
        
        function CellMatrix( params, cells ) {
            var my = this;

            this.params = params;
            this.cells = ( ( typeof cells === "undefined" ) ? [] : cells );
            this.activeCells = [];          // Array of only the active cells
            this.predictedActiveCells = []; // Array of only the active cells which were predicted
            this.learningCells = [];        // Array of only the learning cells
            this.predictiveCells = [];      // Array of only the predictive cells

            this.activeCellHistory = [];          // Reverse-order history of active cells
            this.predictedActiveCellHistory = []; // Reverse-order history of active cells which were predicted
            this.learningCellHistory = [];        // Reverse-order history of learning cells
            this.predictiveCellHistory = [];      // Reverse-order history of predictive cells

            this.getActiveCellStates = function(){
                return this.cells.map(function(cell){
                    return cell.active;
                });
            }

            this.getActiveCellIndexes = function(){
                return this.activeCells.map(function(cell){
                    return cell.index;
                });
            }

            this.getPredictedActiveCellIndexes = function(){
                return this.predictedActiveCells.map(function(cell){
                    return cell.index;
                });
            }
            
            this.getPredictiveCellStates = function(){
                return this.cells.map(function(cell){
                    return cell.predictive ;
                });
            }
            
            /**
             * Resets the active and learning states after saving them to history
             */
            this.resetActiveStates = function() {
                var c, s, cell;
                // Save active cells history
                my.activeCellHistory.unshift( my.activeCells );
                if( my.activeCellHistory.length > my.params.historyLength ) {
                    my.activeCellHistory.length = my.params.historyLength;
                }
                // Save predicted active cells history
                my.predictedActiveCellHistory.unshift( my.predictedActiveCells );
                if( my.predictedActiveCellHistory.length > my.params.historyLength ) {
                    my.predictedActiveCellHistory.length = my.params.historyLength;
                }
                // Reset active cells
                for( c = 0; c < my.activeCells.length; c++ ) {
                    cell = my.activeCells[c];
                    cell.active = false;
                    cell.predictedActive = false;
                    cell.distalLearnSegment = null; // Reset previous distal learn segment
                    cell.apicalLearnSegment = null; // Reset previous apical learn segment
                    // If cell is in a column, clear segment activity (this isn"t used for cells which feed SP)
                    if( cell.column !== null ) {
                        // Clear previous references to segment activity
                        for( s = 0; s < cell.axonSynapses.length; s++ ) {
                            synapse = cell.axonSynapses[s];
                            // Make sure we haven"t already processed this segment"s active synapses list
                            if( synapse.segment.activeSynapses.length > 0 ) {
                                // Save active synapses history, then clear in preparation for new input
                                synapse.segment.activeSynapsesHistory.unshift( synapse.segment.activeSynapses );
                                if( synapse.segment.activeSynapsesHistory.length > my.params.historyLength ) {
                                    synapse.segment.activeSynapsesHistory.length = my.params.historyLength;
                                }
                                synapse.segment.activeSynapses = [];
                                // Save connected synapses history, then clear in preparation for new input
                                synapse.segment.connectedSynapsesHistory.unshift( synapse.segment.connectedSynapses );
                                if( synapse.segment.connectedSynapsesHistory.length > my.params.historyLength ) {
                                    synapse.segment.connectedSynapsesHistory.length = my.params.historyLength;
                                }
                                synapse.segment.connectedSynapses = [];
                                // Save predicted active synapses history, then clear in preparation for new input
                                synapse.segment.predictedActiveSynapsesHistory.unshift( synapse.segment.predictedActiveSynapses );
                                if( synapse.segment.predictedActiveSynapsesHistory.length > my.params.historyLength ) {
                                    synapse.segment.predictedActiveSynapsesHistory.length = my.params.historyLength;
                                }
                                synapse.segment.predictedActiveSynapses = [];
                            }
                        }
                    }
                }
                // Clear active cells array
                my.activeCells = [];
                // Clear predicted active cells array
                my.predictedActiveCells = [];
                // Save learning cells history
                my.learningCellHistory.unshift( my.learningCells );
                if( my.learningCellHistory.length > my.params.historyLength ) {
                    my.learningCellHistory.length = my.params.historyLength;
                }
                // Reset learning cells
                for( c = 0; c < my.learningCells.length; c++ ) {
                    cell = my.learningCells[c];
                    cell.learning = false;
                }
                // Clear learning cells array
                my.learningCells = [];
                return my;  // Allows chaining function calls
            }

            /**
             * Resets the predictictive states after saving them to history
             */
            this.resetPredictiveStates = function() {
                var c, cell;
                // Save predictive cells history
                my.predictiveCellHistory.unshift( my.predictiveCells );
                if( my.predictiveCellHistory.length > my.params.historyLength ) {
                    my.predictiveCellHistory.length = my.params.historyLength;
                }
                // Reset predictive cells
                for( c = 0; c < my.predictiveCells.length; c++ ) {
                    cell = my.predictiveCells[c];
                    cell.predictive = false;
                    cell.distalLearnSegment = null;  // Reset previous distal learn segment
                    cell.apicalLearnSegment = null;  // Reset previous apical learn segment
                }
                // Clear predictive cells array
                my.predictiveCells = [];
                return my;  // Allows chaining function calls
            }

            /**
             * This function clears all references
             */
            this.clear = function() {
                if( my !== null ) {
                    my.cells = null;
                    my.activeCells = null;
                    my.predictedActiveCells = null;
                    my.predictiveCells = null;
                    my.learningCells = null;
                    my.activeCellHistory = null;
                    my.learningCellHistory = null;
                    my.predictiveCellHistory = null;
                    my.params = null;
                    my = null;
                }
            }
        }
        
        function Column( index, cellIndex, cellsPerColumn, layer ) {
	
            this.index = index; // Index of this column in its layer
            this.layer = layer; // Layer containing this column

            this.overlapActive = 0;          // Count of connections with active input cells
            this.overlapPredictedActive = 0; // Count of connections with correctly predicted input cells
            this.score = null;               // How well column matches current input
            this.persistence = 0;

            // Used to calculate persistence decay
            this.initialPersistence = 0;
            this.lastUsedTimestep = 0;

            this.cells = []; // Array of cells in this column

            this.proximalSegment = new Segment( PROXIMAL, null, this );  // Feed-forward input
            this.bestDistalSegment = null;  // Reference to distal segment best matching current input
            this.bestDistalSegmentHistory = [];  // Reverse-order history of best matching distal segments

            this.bestApicalSegment = null;  // Reference to apical segment best matching current input
            this.bestApicalSegmentHistory = [];  // Reverse-order history of best matching apical segments

            // Create the cells for this column
            var c, cell;
            for( c = 0; c < cellsPerColumn; c++ ) {
                cell = new Cell( layer.cellMatrix, cellIndex + c, index, c, this );
                this.cells.push( cell );
            }

        }
        
        function Layer( params, layerType, proximalInputs, distalInput, apicalInput ) {
            var my = this;

            this.columns = [];  // Array of columns contained in this layer
            this.activeColumns = [];   // Array of only the active columns

            this.type = ( ( typeof layerType === "undefined" ) ? TM_LAYER : layerType );
            this.proximalInputs = ( ( typeof proximalInputs === "undefined" ) ? [] : proximalInputs ); // Feed-forward input cells
            this.distalInput = ( ( typeof distalInput === "undefined" ) ? null : distalInput ); // distal input cells
            this.apicalInput = ( ( typeof apicalInput === "undefined" ) ? null : apicalInput ); // apical input cells

            this.params = params;
            this.cellMatrix = new CellMatrix( this.params ); // A matrix containing all cells in the layer

            this.timestep = 0; // Used for tracking least recently used resources

            // Calculate the decay constant
            // (avoids repeating these calculation numerous times when simulating decay)
            if( ( typeof this.params.meanLifetime !== "undefined" ) && ( this.params.meanLifetime > 0 ) ) {
                this.params.decayConstant = ( 1.0 / parseFloat( this.params.meanLifetime ) );
            }

            /**
             * This function adds a new column to the layer, and creates all of
             * the cells in it.  If skipSpatialPooling is false, it also
             * establishes randomly distributed proximal connections with the
             * input cells.
             */
            this.addColumn = function() {
                var i, c, p, input, perm, synapse;
                var column = new Column( my.columns.length, my.columns.length * my.params.cellsPerColumn, my.params.cellsPerColumn, my );

                // Randomly connect columns to input cells, for use in spatial pooling
                if( !my.params.skipSpatialPooling ) {
                    for( i = 0; i < my.proximalInputs.length; i++ ) {
                        input = my.proximalInputs[i];
                        for( c = 0; c < input.cells.length; c++ ) {
                            p = Math.floor( Math.random() * 100 );
                            if( p < my.params.potentialPercent ) {
                                perm = Math.floor( Math.random() * 100 );
                                if( perm > my.params.connectedPermanence ) {
                                    // Start with weak connections (for faster initial learning)
                                    perm = my.params.connectedPermanence;
                                }
                                synapse = new Synapse( input.cells[c], column.proximalSegment, perm );
                            }
                        }
                    }
                }

                my.columns.push( column );
                return column;
            }

            // Add the columns if spatial pooling is enabled
            if( !this.params.skipSpatialPooling ) {
                for( var c = 0; c < this.params.columnCount; c++ ) {
                    this.addColumn();
                }
            }

            /**
             * This function clears all references
             */
            this.clear = function() {
                if( my !== null ) {
                    my.cellMatrix.clear();
                    my.cellMatrix = null;
                    my.columns = null;
                    my.activeColumns = null;
                    my.proximalInputs = null;
                    my.distalInput = null;
                    my.apicalInput = null;
                    my.params = null;
                    my.timestep = null;
                    my = null;
                }
            }

        }
        
        function Segment( type, cellRx, column ) {

            this.type = type; // proximal, distal, or apical
            this.cellRx = cellRx;  // Receiving cell
            this.column = ( ( typeof column === "undefined" ) ? null : column );

            this.lastUsedTimestep = 0;  // Used to remove least recently used segment if max per cell is exceeded
            this.synapses = [];  // Connections to axons of transmitting cells
            this.activeSynapses = [];  // both connected and potential synapses
            this.connectedSynapses = [];  // connected synapses only
            this.predictedActiveSynapses = [];  // synapses receiving input from predicted active cells
            this.activeSynapsesHistory = [];  // Reverse-order history of active synapses
            this.connectedSynapsesHistory = [];  // Reverse-order history of connected synapses
            this.predictedActiveSynapsesHistory = [];  // Reverse-order history of synapses receiving input from predicted active cells

            this.active = false;
            this.learning = false;

            if( this.cellRx !== null ) {
                if( this.type == DISTAL ) {
                    this.cellRx.distalSegments.push( this );
                } else if( this.type == APICAL ) {
                    this.cellRx.apicalSegments.push( this );
                } else {
                    this.cellRx.proximalSegments.push( this );
                }
            }
        }
        
        function Synapse( cellTx, segment, permanence ) {

            this.cellTx = cellTx;  // Transmitting cell
            this.segment = segment; // Dendrite segment of receiving cell
            this.permanence = permanence;  // Connection strength

            // Let the transmitting cell and receiving segment know about this synapse
            this.segment.synapses.push( this );
            this.cellTx.axonSynapses.push( this );
        }
        
        // Defaults to use for any param not specified:
        this.defaultParams = {
            "columnCount"                 :  2048,
            "cellsPerColumn"              :    32,
            "activationThreshold"         :    15,
            "initialPermanence"           :    31,  // %
            "connectedPermanence"         :    40,  // %
            "minThreshold"                :    10,
            "maxNewSynapseCount"          :    32,
            "permanenceIncrement"         :    15,  // %
            "permanenceDecrement"         :    10,  // %
            "predictedSegmentDecrement"   :     1,  // %
            "maxSegmentsPerCell"          :   128,
            "maxSynapsesPerSegment"       :   128,
            "potentialPercent"            :    50,  // %
            "sparsity"                    :     2,  // %
            "inputCellCount"              :  1024,
            "skipSpatialPooling"          : false,
            "historyLength"               :     4,
            // Temporal Pooling parameters
            "tpSparsity"                  :     10,  // %
            "meanLifetime"                :     4,
            "excitationMin"               :     10,
            "excitationMax"               :     20,
            "excitationXMidpoint"         :     5,
            "excitationSteepness"         :     1,
            "weightActive"                :     1,
            "weightPredictedActive"       :     4,
            "forwardPermananceIncrement"  :     2,
            "backwardPermananceIncrement" :     1
        };

        /**
         * This function creates a cell matrix containing the number of
         * input cells specifed in the params, and returns it.
         */
        this.createInputCells = function( params ) {
            var i, cell;
            // Create a matrix to hold the new cells
            var inputCells = new CellMatrix( params );
            // Generate the specified number of input cells
            for( i = 0; i < params.inputCellCount; i++ ) {
                cell = new Cell( inputCells, i );
            }
            // Return the cell matrix
            return inputCells;
        }

        /**
         * This function generates a new layer.  If spatial pooling is enabled
         * and an input layer is not specified, a matrix of input cells is also
         * created, containing the cell count specified in the params.
         * 
         * TM_LAYER is a layer which receives distal input from its own cells.
         * TP_LAYER is a layer which produces stable representations.
         */
        this.createLayer = function( params, layerType, inputLayerIdx ) {
            var property;
            var type = ( ( typeof layerType === "undefined" ) ? TM_LAYER : layerType );
            var inputLayer = ( ( typeof inputLayerIdx === "undefined" ) ? null : my.layers[inputLayerIdx] );

            // Start with a copy of the default params
            var layerParams = [];
            for( property in my.defaultParams ) {
                if( my.defaultParams.hasOwnProperty( property ) ) {
                    layerParams[property] = my.defaultParams[property];
                }
            }
            // Override default params with any provided
            if( ( typeof params !== "undefined" ) && ( params !== null ) ) {
                for( property in params ) {
                    if( params.hasOwnProperty( property ) ) {
                        layerParams[property] = params[property];
                    }
                }
            }

            // Determine where feed-forward input should come from
            var inputCells = null;
            if( inputLayer !== null ) {
                // Input coming from another layer
                inputCells = inputLayer.cellMatrix;
            } else if( !layerParams.skipSpatialPooling ) {
                // Create a new matrix of input cells
                inputCells = my.createInputCells( layerParams );
            }
            // Create the layer
            var layer = new Layer( layerParams, layerType, [inputCells] );

            if( type == TM_LAYER || type == TP_LAYER ) {
                // TM and TP layers receive distal input from their own cell matrix
                layer.distalInput = layer.cellMatrix;
            }

            my.layers.push( layer ); // Save for easy lookup

            return my; // Allows chaining function calls
        }

        /**
         * This function increments a layer"s timestep and activates its columns which
         * best match the input.  If learning is enabled, adjusts the columns to better
         * match the input.
         * 
         * This function also performs temporal pooling if layer is configured as such.
         * 
         * Note: The active input SDRs must align with the proximal input cell matrices
         * in the layer.
         */
        this.spatialPooling = function( layerIdx, activeInputSDRs, learningEnabled ) {
            var c, i, randomIndexes, input, indexes, synapse, column, cell;
            var learn = ( ( typeof learningEnabled === "undefined" ) ? false : learningEnabled );
            var layer = my.layers[layerIdx];
            //throw JSON.stringify(activeInputSDRs);
            layer.timestep++;

            // If we were given activeInputSDRs, update input cell activity to match
            if( activeInputSDRs.length > 0 ) {
                // Clear input cell active states
                for( i = 0; i < layer.proximalInputs.length; i++ ) {
                    layer.proximalInputs[i].resetActiveStates();
                }

                // Update active state of input cells which match the specified SDR.
                // If learning is enabled, also set their learn state.
                for( i = 0; i < activeInputSDRs.length; i++ ) {
                    indexes = activeInputSDRs[i];
                    input = layer.proximalInputs[i];
                    for( c = 0; c < indexes.length; c++ ) {
                        cell = input.cells[indexes[c]];
                        cell.active = true;
                        input.activeCells.push( cell );
                        // If cell was predicted, add to predictedActive list as well
                        if( cell.predictive ) {
                            cell.predictedActive = true;
                            input.predictedActiveCells.push( cell );
                        }
                        if( learn ) { // Learning enabled, set learn states
                            cell.learning = true;
                            input.learningCells.push( cell );
                        }
                    }
                }

                // Clear input cell predictive states
                for( i = 0; i < layer.proximalInputs.length; i++ ) {
                    layer.proximalInputs[i].resetPredictiveStates();
                }

                // Activate the input cells (may generate new predictions)
                for( i = 0; i < activeInputSDRs.length; i++ ) {
                    input = layer.proximalInputs[i];
                    // Activate input cells (also generates new column scores)
                    my.activateCellMatrix( input, layer.timestep );
                }
            }

            // Select the columns with the highest scores to become active
            var bestColumns = [];
            var activeColumnCount = parseInt( ( parseFloat( layer.params.sparsity ) / 100 ) * layer.params.columnCount );
            if( activeColumnCount < 1 ) {
                activeColumnCount = 1;
            }
            for( i = 0; i < layer.columns.length; i++ ) {
                column = layer.columns[i];
                // Calculate the column score
                if( column.score === null ) {
                    if( layer.type == TM_LAYER ) {
                        // For TM layers, this is just the overlap with active input cells
                        column.score = column.overlapActive;
                    } else if( layer.type == TP_LAYER ) {
                        // For TP layers, use a weighted average of overlap with active and predicted active cells
                        column.score = ( parseFloat( column.overlapActive ) * parseFloat( layer.params.weightActive ) )
                            + ( parseFloat( column.overlapPredictedActive ) * parseFloat( layer.params.weightPredictedActive ) );
                    }
                }
                // Check if this column has a higher score than what has already been chosen
                for( c = 0; c < activeColumnCount; c++ ) {
                    // If bestColumns array is not full, or if score is better, add it
                    if( ( !( c in bestColumns ) ) || bestColumns[c].score < column.score ) {
                        bestColumns.splice( c, 0, column );
                        // Don"t let bestColumns array grow larger than activeColumnCount
                        if( bestColumns.length > activeColumnCount ) {
                            bestColumns.length = activeColumnCount;
                        }
                        break;
                    }
                }
            }

            for( i = 0; i < activeColumnCount; i++ ) {
                column = bestColumns[i];
                if( layer.type == TP_LAYER ) {
                    // Increase the column persistence based on overlap with correctly predicted inputs
                    column.persistence = my.excite( column.persistence, column.overlapPredictedActive,
                        layer.params.excitationMin, layer.params.excitationMax, layer.params.excitationXMidpoint, layer.params.excitationSteepness );
                    column.initialPersistence = column.persistence;
                }
                column.lastUsedTimestep = layer.timestep;
                // SP learning
                if( learn ) {
                    for( c = 0; c < column.proximalSegment.synapses.length; c++ ) {
                        synapse = column.proximalSegment.synapses[c];
                        // For TM layers, enforce all active cells.  For TP layers, only correctly predicted cells
                        if(
                            ( ( layer.type == TM_LAYER ) && synapse.cellTx.active )
                            || ( ( layer.type == TP_LAYER ) && synapse.cellTx.predictedActive )
                        ) {
                            synapse.permanence += layer.params.permanenceIncrement;
                            if( synapse.permanence > 100 ) {
                                synapse.permanence = 100;
                            }
                        } else {
                            synapse.permanence -= layer.params.permanenceDecrement;
                            if( synapse.permanence < 0 ) {
                                synapse.permanence = 0;
                            }
                        }
                    }
                }
            }

            // Activated columns for a TP layer are those with highest persistence
            if( layer.type == TP_LAYER ) {
                // Clear the "bestColumns" array so it can be rebuilt.
                bestColumns = [];
                // Calculate a new active column count based on TP sparsity param
                activeColumnCount = parseInt( ( parseFloat( layer.params.tpSparsity ) / 100 ) * layer.params.columnCount );
                if( activeColumnCount < 1 ) {
                    activeColumnCount = 1;
                }
            }

            // Post-processing, cleanup
            for( i = 0; i < layer.columns.length; i++ ) {
                column = layer.columns[i];
                if( layer.type == TP_LAYER ) {
                    // Generate a new set of "best columns" based on persistence values
                    for( c = 0; c < activeColumnCount; c++ ) {
                        // If bestColumns array is not full, or if score is better, add it
                        if( ( !( c in bestColumns ) ) || bestColumns[c].persistence < column.persistence ) {
                            // Only use column if it has some persistence
                            if( column.persistence > 0 ) {
                                bestColumns.splice( c, 0, column );
                                // Don"t let bestColumns array grow larger than activeColumnCount
                                if( bestColumns.length > activeColumnCount ) {
                                    bestColumns.length = activeColumnCount;
                                }
                            }
                            break;
                        }
                    }
                    // Decay persistence value
                    column.persistence = my.decay( layer.params.decayConstant,
                        column.initialPersistence, layer.timestep - column.lastUsedTimestep );
                }
                // Reset overlap scores
                column.overlapActive = 0;
                column.overlapPredictedActive = 0;
                column.score = null;
            }

            layer.activeColumns = bestColumns;


            // TODO: Forward learning

            // TODO: Backward learning


            return my; // Allows chaining function calls
        }

        /**
         * This function activates cells in the active columns, generates predictions, and
         * if learning is enabled, learns new temporal patterns.
         */
        this.temporalMemory = function( layerIdx, learningEnabled ) {
            var learn = ( ( typeof learningEnabled === "undefined" ) ? false : learningEnabled );
            var layer = my.layers[layerIdx];

            // Phase 1: Activate
            my.tmActivate( layer, learn );

            // Phase 2: Predict
            my.tmPredict( layer );

            // Phase 3: Learn
            if( learn ) {
                my.tmLearn( layer );
            }
            return my; // Allows chaining function calls
        }

        /**
         * This function allows the input cells to grow apical connections with the active cells in
         * the specified layer, allowing next inputs to be predicted.  This is designed to replace
         * the heavier-weight classifier logic for making predictions one timestep in the future.
         */
        this.inputMemory = function( layerIdx ) {
            var i;
            var layer = my.layers[layerIdx];

            for( i = 0; i < layer.proximalInputs.length; i++ ) {
                my.trainCellMatrix( layer.cellMatrix, layer.proximalInputs[i], APICAL, layer.timestep );
            }
        }

        /**
         * Activates cells in each active column, and selects cells to learn in the next
         * timestep.  Activity is queued up, but not transmitted to receiving cells until
         * tmPredict() is executed.
         * 
         * This is Phase 1 of the temporal memory process.
         */
        this.tmActivate = function( layer, learn ) {
            var i, c, x, predicted, column, cell, learningCell, synapse;

            // Reset this layer"s active cell states after saving history.
            layer.cellMatrix.resetActiveStates();

            // Loop through each active column and activate cells
            for( i = 0; i < layer.activeColumns.length; i++ ) {
                column = layer.activeColumns[i];
                predicted = false;
                for( c = 0; c < column.cells.length; c++ ) {
                    cell = column.cells[c];
                    if( cell.predictive ) {
                        cell.active = true; // Activate predictive cell
                        layer.cellMatrix.activeCells.push( cell );
                        cell.predictedActive = true;
                        layer.cellMatrix.predictedActiveCells.push( cell );
                        if( learn ) {
                            cell.learning = true;  // Flag cell for learning
                            layer.cellMatrix.learningCells.push( cell );
                        }
                        predicted = true;  // Input was predicted
                    }
                }
                if( !predicted ) {
                    // Input was not predicted, activate all cells in column
                    for( c = 0; c < column.cells.length; c++ ) {
                        cell = column.cells[c];
                        cell.active = true;
                        layer.cellMatrix.activeCells.push( cell );
                    }
                    if( learn ) {
                        // Select a cell for learning
                        if( column.bestDistalSegment === null ) {
                            // No segments matched the input, pick least used cell to learn
                            x = Math.floor( Math.random() * column.cells.length );
                            learningCell = column.cells[x];  // Start with a random cell
                            // Loop through all cells to find one with fewest segments
                            for( c = 0; c < column.cells.length; c++ ) {
                                cell = column.cells[x];
                                if( cell.distalSegments.length < learningCell.distalSegments.length ){
                                    learningCell = cell;  // Fewer segments, use this one
                                }
                                x++;
                                if( x >= column.cells.length ) {
                                    x = 0; // Wrap around to beginning of cells array
                                }
                            }
                            learningCell.learning = true;  // Flag chosen cell to learn
                            layer.cellMatrix.learningCells.push( learningCell );
                        } else {
                            // Flag cell with best matching segment to learn
                            column.bestDistalSegment.cellRx.learning = true;
                            layer.cellMatrix.learningCells.push( column.bestDistalSegment.cellRx );
                        }
                    }
                }
            }
        }

        /**
         * Transmits queued activity, driving cells into predictive state based on
         * distal or apical connections with active cells.  Also identifies the
         * distal and apical segments that best match the current activity, which
         * is later used when tmLearn() is executed.
         * 
         * This is Phase 2 of the temporal memory process.
         */
        this.tmPredict = function( layer ) {
            var i, c, column, cell, synapse;

            // Reset this layer"s predictive cell states after saving history.
            layer.cellMatrix.resetPredictiveStates();

            // Save column best matching segments history, and clear references
            for( i = 0; i < layer.columns.length; i++ ) {
                // Save best matching distal segment history
                column = layer.columns[i];
                column.bestDistalSegmentHistory.unshift( column.bestDistalSegment );
                if( column.bestDistalSegmentHistory.length > layer.params.historyLength ) {
                    column.bestDistalSegmentHistory.length = layer.params.historyLength;
                }
                // Clear reference to best matching distal segment
                column.bestDistalSegment = null;
                // Save best matching apical segment history
                column.bestApicalSegmentHistory.unshift( column.bestApicalSegment );
                if( column.bestApicalSegmentHistory.length > layer.params.historyLength ) {
                    column.bestApicalSegmentHistory.length = layer.params.historyLength;
                }
                // Clear reference to best matching apical segment
                column.bestApicalSegment = null;
            }

            // Transmit queued activity to receiving synapses to generate predictions
            my.activateCellMatrix( layer.cellMatrix, layer.timestep );
        }

        /**
         * This function allows cells in a layer to grow distal connections with other cells
         * in the same layer, allowing next state to be predicted. Enforces good predictions
         * and degrades wrong predictions.
         * 
         * This is Phase 3 of the temporal memory process.
         */
        this.tmLearn = function( layer ) {

            my.trainCellMatrix( layer.distalInput, layer.cellMatrix, DISTAL, layer.timestep );
        }

        /**
         * Activates the cells in a matrix which have had their "active" flag set.
         * If cells are feeding a spatial pooler, increases the scores of the columns
         * they are connected to.  Otherwise, transmits to dendrites of other receiving
         * cells, and may place them into predictive or active states.
         */
        this.activateCellMatrix = function( cellMatrix, timestep ) {
            var c, s, column, cell, synapse;

            for( c = 0; c < cellMatrix.activeCells.length; c++ ) {
                cell = cellMatrix.activeCells[c];
                // Activate synapses along the cell"s axon
                for( s = 0; s < cell.axonSynapses.length; s++ ) {
                    synapse = cell.axonSynapses[s];
                    synapse.segment.lastUsedTimestep = timestep; // Update segment"s last used timestep
                    if( synapse.segment.cellRx === null ) {
                        // This is the proximal segment of a column.  Just update the column score.
                        if( synapse.permanence >= cellMatrix.params.connectedPermanence ) {
                            synapse.segment.column.overlapActive++;
                            if( cell.predictedActive ) {
                                synapse.segment.column.overlapPredictedActive++;
                            }
                        }
                    } else {
                        // This is the segment of a cell.  Determine if state should be updated.
                        // First, add to segment"s active synapses list
                        synapse.segment.activeSynapses.push( synapse );
                        if( cell.predictedActive ) {
                            // Transmitting cell was correctly predicted, add synapse to predicted active list
                            synapse.segment.predictedActiveSynapses.push( synapse );
                        }
                        if( synapse.permanence >= cellMatrix.params.connectedPermanence ) {
                            // Synapse connected, add to connected synapses list
                            synapse.segment.connectedSynapses.push( synapse );
                            if( synapse.segment.connectedSynapses.length >= cellMatrix.params.activationThreshold ) {
                                // Number of connected synapses above threshold. Update receiving cell.
                                if( !synapse.segment.cellRx.predictive ) {
                                    // Mark receiving cell as predictive (TODO: consider proximal segments)
                                    synapse.segment.cellRx.predictive = true;
                                    // Update the receiving cell"s matrix
                                    synapse.segment.cellRx.matrix.predictiveCells.push( synapse.segment.cellRx );
                                    // Add segment to appropriate list for learning
                                    if( synapse.segment.type == DISTAL ) {
                                        synapse.segment.cellRx.distalLearnSegment = synapse.segment;
                                    } else if( synapse.segment.type == APICAL ) {
                                        // TODO: Consider cases where distal + apical should activate cell.
                                        synapse.segment.cellRx.apicalLearnSegment = synapse.segment;
                                    }
                                }
                            }
                        }
                        // If receiving cell is in a column, update best matching segment references
                        if( synapse.segment.cellRx.column !== null ) {
                            column = synapse.segment.cellRx.column;
                            // Save a reference to the best matching distal and apical segments in the column
                            if( synapse.segment.type === DISTAL ) {
                                if( ( column.bestDistalSegment === null )
                                    || ( synapse.segment.connectedSynapses.length > column.bestDistalSegment.connectedSynapses.length )
                                    || ( synapse.segment.activeSynapses.length > column.bestDistalSegment.activeSynapses.length ) )
                                {
                                    // Make sure segment has at least minimum number of potential synapses
                                    if( synapse.segment.activeSynapses.length >= cellMatrix.params.minThreshold ) {
                                        // This segment is a better match, use it
                                        column.bestDistalSegment = synapse.segment;
                                        synapse.segment.cellRx.distalLearnSegment = synapse.segment;
                                    }
                                }
                            } else if( synapse.segment.type === APICAL ) {
                                if( ( column.bestApicalSegment === null )
                                    || ( synapse.segment.connectedSynapses.length > column.bestApicalSegment.connectedSynapses.length )
                                    || ( synapse.segment.activeSynapses.length > column.bestApicalSegment.activeSynapses.length ) )
                                {
                                    // Make sure segment has at least minimum number of potential synapses
                                    if( synapse.segment.activeSynapses.length >= cellMatrix.params.minThreshold ) {
                                        // This segment is a better match, use it
                                        column.bestApicalSegment = synapse.segment;
                                        synapse.segment.cellRx.apicalLearnSegment = synapse.segment;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        /**
         * Creates or adapts distal and apical segments in a receiving cell matrix to
         * align with previously active cells in a transmitting cell matrix. Enforces
         * good predictions and degrades wrong predictions.
         */
        this.trainCellMatrix = function( cellMatrixTx, cellMatrixRx, inputType, timestep ) {
            var c, s, p, sourcePredicted, randomIndexes, cell, segment, synapse;

            if( ( cellMatrixTx.activeCellHistory.length > 0 ) && ( cellMatrixRx.predictiveCellHistory.length > 0 ) ) {
                // Enforce correct predictions, degrade wrong predictions
                for( c = 0; c < cellMatrixRx.predictiveCellHistory[0].length; c++ ) {
                    segment = null;
                    cell = cellMatrixRx.predictiveCellHistory[0][c];
                    if( cell.column !== null ) {
                        // Cell is part of a layer"s cell matrix.
                        // Make sure this cell is the one referenced by column"s best segment history
                        if( inputType == DISTAL
                            && cell.column.bestDistalSegmentHistory.length > 0
                            && cell.column.bestDistalSegmentHistory[0] !== null
                            && cell.column.bestDistalSegmentHistory[0].cellRx === cell )
                        {
                            segment = cell.column.bestDistalSegmentHistory[0];
                        } else if( inputType == APICAL
                            && cell.column.bestApicalSegmentHistory.length > 0
                            && cell.column.bestApicalSegmentHistory[0] !== null
                            && cell.column.bestApicalSegmentHistory[0].cellRx === cell )
                        {
                            segment = cell.column.bestApicalSegmentHistory[0];
                        }
                    } else {
                        // Cell is part of an input cell matrix.
                        if( inputType == DISTAL ) {
                            segment = cell.distalLearnSegment;
                        } else if( inputType == APICAL ) {
                            segment = cell.apicalLearnSegment;
                        }
                    }
                    if( segment !== null
                        && segment.activeSynapsesHistory.length > 0
                        && segment.activeSynapsesHistory[0].length > 0 )
                    {
                        if( cell.active ) {
                            // Correct prediction.  Train it to better align with activity.
                            my.trainSegment( segment, cellMatrixTx.learningCellHistory[0], cellMatrixRx.params, timestep );
                        } else {
                            // Wrong prediction.
                            for( s = 0; s < segment.synapses.length; s++ ) {
                                synapse = segment.synapses[s];
                                // Check if transmitting cell was itself predicted
                                sourcePredicted = false;
                                if( segment.predictedActiveSynapsesHistory.length > 0 ) {
                                    for( p = 0; p < segment.predictedActiveSynapsesHistory[0].length; p++ ) {
                                        if( segment.predictedActiveSynapsesHistory[0][p] === synapse ) {
                                            sourcePredicted = true;
                                        }
                                    }
                                }
                                // Only punish wrong predictions if the source minicolumn was not bursting (fixes some undesirable forgetfulness)
                                if( sourcePredicted ) {
                                    // Degrade this connection.
                                    synapse.permanence -= cellMatrixRx.params.predictedSegmentDecrement;
                                    if( synapse.permanence < 0 ) {
                                        synapse.permanence = 0;
                                    }
                                }
                            }
                        }
                    }
                    cell.learning = false;  // Remove learning flag, so cell doesn"t get double-trained
                }
                // If this isn"t first input (or reset), train cells which were not predicted
                if( cellMatrixRx.learningCellHistory[0].length > 0 ) {
                    // Loop through cells which have been flagged for learning
                    for( c = 0; c < cellMatrixRx.learningCells.length; c++ ) {
                        segment = null;
                        cell = cellMatrixRx.learningCells[c];

                        // Make sure we haven"t already trained this cell
                        if( cell.learning ) {
                            if( cell.column !== null ) {
                                // Cell is part of a layer"s cell matrix
                                if( inputType == DISTAL
                                    && cell.column.bestDistalSegmentHistory.length > 0
                                    && cell.column.bestDistalSegmentHistory[0] !== null
                                    && cell.column.bestDistalSegmentHistory[0].cellRx === cell )
                                {
                                    segment = cell.column.bestDistalSegmentHistory[0];
                                }else if( inputType == APICAL
                                    && cell.column.bestApicalSegmentHistory.length > 0
                                    && cell.column.bestApicalSegmentHistory[0] !== null
                                    && cell.column.bestApicalSegmentHistory[0].cellRx === cell )
                                {
                                    segment = cell.column.bestApicalSegmentHistory[0];
                                }
                            } else {
                                // Cell is part of an input cell matrix
                                if( inputType == DISTAL ) {
                                    segment = cell.distalLearnSegment;
                                } else if( inputType == APICAL ) {
                                    segment = cell.apicalLearnSegment;
                                }
                            }
                            // We haven"t trained this cell yet.  Check if it had a matching segment
                            if( segment !== null
                                && segment.activeSynapsesHistory.length > 0
                                && segment.activeSynapsesHistory[0].length > 0 )
                            {
                                // Found a matching segment.  Train it to better align with activity.
                                my.trainSegment( segment, cellMatrixTx.learningCellHistory[0], cellMatrixRx.params, timestep );
                            } else {
                                // No matching segment.  Create a new one.
                                segment = new Segment( inputType, cell, cell.column );
                                segment.lastUsedTimestep = timestep;
                                // Connect segment with random sampling of previously active learning cells, up to max new synapse count
                                randomIndexes = my.randomIndexes( cellMatrixTx.learningCellHistory[0].length, cellMatrixRx.params.maxNewSynapseCount, false );
                                for( s = 0; s < randomIndexes.length; s++ ) {
                                    synapse = new Synapse( cellMatrixTx.learningCellHistory[0][randomIndexes[s]], segment, cellMatrixRx.params.initialPermanence );
                                }
                            }
                            cell.learning = false;
                        }
                    }
                }
            }
        }

        /**
         * Trains a segment of any type to better match the specified active cells.
         * Active synapses are enforced, inactive synapses are degraded, and new synapses are formed
         * with a random sampling of the active cells, up to max new synapses.
         */
        this.trainSegment = function( segment, activeCells, params, timestep ) {
            var s, i, synapse, segments, segmentIndex, lruSegmentIndex;
            var randomIndexes = my.randomIndexes( activeCells.length, params.maxNewSynapseCount, false );
            var inactiveSynapses = segment.synapses.slice();  // Inactive synapses (will remove active ones below)
            // Enforce synapses that were active
            if( segment.activeSynapsesHistory.length > 0 ) {
                for( s = 0; s < segment.activeSynapsesHistory[0].length; s++ ) {
                    synapse = segment.activeSynapsesHistory[0][s];
                    synapse.permanence += params.permanenceIncrement;
                    if( synapse.permanence > 100 ) {
                        synapse.permanence = 100;
                    }
                    // Remove cell from random sampling if present (prevents duplicate connections)
                    for( i = 0; i < randomIndexes.length; i++ ) {
                        if( activeCells[randomIndexes[i]].index == synapse.cellTx.index ) {
                            // Cell is in the random sampling, remove it
                            randomIndexes.splice( i, 1 );
                            break;
                        }
                    }
                    // Remove synapse from the list of inactive synapses
                    for( i = 0; i < inactiveSynapses.length; i++ ) {
                        if( inactiveSynapses[i] === synapse ) {
                            // Found it
                            inactiveSynapses.splice( i, 1 );
                            break;
                        }
                    }
                }
            }
            // Degrade synapses that were not active
            for( s = 0; s < inactiveSynapses.length; s++ ) {
                synapse = inactiveSynapses[s];
                synapse.permanence -= params.permanenceDecrement;
                if( synapse.permanence < 0 ) {
                    synapse.permanence = 0;
                    // TODO: Delete synapse to free resources
                }
            }
            // Select the relevant list of segments, based on type
            if( segment.type == DISTAL ) {
                segments = segment.cellRx.distalSegments;
            } else if( segment.type == APICAL ) {
                segments = segment.cellRx.apicalSegments;
            } else {
                segments = segment.cellRx.proximalSegments;
            }
            if( segment.activeSynapsesHistory[0].length < params.maxNewSynapseCount ) {
                // Connect segment with random sampling of previously active cells, up to max new synapse count
                for( i = 0; i < randomIndexes.length; i++ ) {
                    if( segment.synapses.length >= params.maxSynapsesPerSegment ) {
                        // Cannot add any more synapses to this segment.  Check if we can add a new segment.
                        if( segments.length >= params.maxSegmentsPerCell ) {
                            // Cannot add any more segments to this cell.  Select least recently used and remove it.
                            segmentIndex = Math.floor( Math.random() * segments.length );
                            lruSegmentIndex = segmentIndex;  // Start with a random segment index
                            // Loop through segments to find least recently used
                            for( s = 0; s < segments.length; s++ ) {
                                segmentIndex++;
                                if( segmentIndex >=  segments.length ) {
                                    segmentIndex = 0;  // Wrap back around to beginning of list
                                }
                                // Check if this segment is less recently used than selected one
                                if( segments[segmentIndex].lastUsedTimestep < segments[lruSegmentIndex].lastUsedTimestep ) {
                                    lruSegmentIndex = segmentIndex;  // Used less recently.. select this one instead
                                }
                            }
                        }
                        // Add new segment to this cell
                        segment = new Segment( segment.type, segment.cellRx, segment.cellRx.column );
                        segment.lastUsedTimestep = timestep;
                    }
                    // Add new synapse to this segment
                    synapse = new Synapse( activeCells[randomIndexes[i]], segment, params.initialPermanence );
                }
            }
        }

        /**
         * Returns an array of size "resultCount", containing unique indexes in the range (0, length - 1)
         * If "ordered" is true, indexes will be in sequential order starting from a random position
         * If "ordered" is false, indexes will be in random order
         */
        this.randomIndexes = function( length, resultCount, ordered ) {
            var i1, i2;
            var results = [];  // Array to hold the random indexes
            var rc = resultCount;
            // Make sure not to return more results than there are available
            if( rc > length ) {
                rc = length;
            }
            if( ordered ) {
                // Start at a random index
                i1 = Math.floor( Math.random() * length );
                // Capture indexes in order from this point
                for( i2 = 0; i2 < rc; i2++ ) {
                    results.push( i1 );
                    i1++;
                    if( i1 >= length ) {
                        // End of list, loop back around to beginning
                        i1 = 0;
                    }
                }
            } else {
                // Create an array to hold unprocessed indexes
                var indexes = [];
                for( i1 = 0; i1 < length; i1++ ) {
                    indexes.push( i1 );
                }
                // Capture random indexes out of order
                for( i2 = 0; i2 < rc; i2++ ) {
                    // Pick a random element from the unprocessed list
                    i1 = Math.floor( Math.random() * ( length - i2 ) );
                    // Capture the index in this element
                    results.push( indexes[i1] );
                    // Remove it from the unprocessed list
                    indexes.splice( i1, 1 );
                }
            }
            return results;
        }

        /**
         * This function calculates an exponential decay
         * 
         * @param decayConstant: 1/meanLifetime
         */
        this.decay = function( decayConstant, initialValue, timesteps ) {
            return ( Math.exp( -decayConstant * timesteps ) * initialValue );
        }

        /**
         * This function calculates a logistic excitement based on overlap
         */
        this.excite = function( currentValue, overlap, minValue, maxValue, xMidpoint, steepness ) {
            return ( currentValue + ( maxValue - minValue ) / ( 1 + Math.exp( -steepness * ( overlap - xMidpoint ) ) ) );
        }

        /**
         * This function clears all layers
         */
        this.clear = function() {
            // Loop through all saved layers
            var i;
            for( i = 0; i < my.layers.length; i++ ) {
                my.layers[i].clear(); // Clears all references
            }
            my.layers = []; // Empty the layers array
            return my; // Allows chaining function calls
        }

    },

    processRow: function (row, rowWriter, context) {
    
        
		// Activate TM layer
        this.htmController.spatialPooling( 0, [row.INPUTS], true );
        this.htmController.temporalMemory( 0, true );

        // Activate TP layer (skipping this step for demonstration purposes)
        //this.htmController.spatialPooling( 1, [], true );
        //this.htmController.temporalMemory( 1, true );

        // Train cells in input layer to predict next input
        this.htmController.inputMemory( 0 );
    
        rowWriter.writeRow({"ACTIVE": this.htmController.layers[0].cellMatrix.getActiveCellIndexes(), "PREDICTIVE": this.htmController.layers[0].cellMatrix.getPredictedActiveCellIndexes()});
      
    },
    finalize: function (rowWriter, context) {
    
    },
    initialize: function(argumentInfo, context) {
      // each of these variables will contain a map of cluster_no to running total
      this.htmController=new this.HTMController();
      this.htmController.createLayer( {}, 0 );

      // Create the pooling layer (not needed for this demo)
      //this.htmController.createLayer( {}, 1, [0] );

    }}';
