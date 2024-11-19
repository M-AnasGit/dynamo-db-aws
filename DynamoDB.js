const {
	DynamoDBClient,
	GetItemCommand,
	BatchGetItemCommand,
	PutItemCommand,
	BatchWriteItemCommand,
	QueryCommand,
	ScanCommand,
	UpdateItemCommand,
	DeleteItemCommand,
} = require('@aws-sdk/client-dynamodb')

class HttpError extends Error {
	constructor(message, statusCode) {
		super(message)
		this.statusCode = statusCode
		this.name = this.constructor.name
	}
}

class DynamoDB {
	/**
	 * Constructor to initialize DynamoDB client with configuration and optional dev flag.
	 *
	 * @param {Object} dbconfig - Configuration object for DynamoDB client.
	 * @param {boolean} dev - Flag to enable or disable development mode (logging, etc.).
	 */
	constructor(dbconfig = {}, dev = false) {
		this.client = new DynamoDBClient(dbconfig) // Initialize the DynamoDB client
		this.dev = dev // Flag to enable or disable development-specific behavior
	}

	/**
	 * Set an item in a DynamoDB table.
	 *
	 * @param {string} table - The name of the DynamoDB table.
	 * @param {Object} item - The item to store in the table.
	 * @param {string|null} condition - Optional condition expression for conditional writes.
	 * @returns {Object} - The item that was successfully added to the table.
	 * @throws {HttpError} - Throws HttpError if there is an error or the item already exists.
	 */
	async set(table, item, condition = null) {
		const params = {
			TableName: table,
			Item: item,
		}

		if (condition) {
			params.ConditionExpression = condition
		}

		try {
			const command = new PutItemCommand(params)
			await this.client.send(command)
			return item
		} catch (error) {
			if (error.name === 'ConditionalCheckFailedException') {
				if (this.dev) console.log('Item already exists in database')
				throw new HttpError('Item already exists in database', 409)
			} else {
				// Handle general errors
				if (this.dev) console.log('Error setting item in database: ', error)
				throw new HttpError('Error setting item in database', 500)
			}
		}
	}

	/**
	 * Get an item from a DynamoDB table by its key.
	 *
	 * @param {string} table - The name of the DynamoDB table.
	 * @param {Object} keys - The keys to identify the item in the table.
	 * @returns {Object} - The item retrieved from the table.
	 * @throws {HttpError} - Throws HttpError if the item does not exist.
	 */
	async get(table, keys) {
		const params = {
			TableName: table, // DynamoDB table name
			Key: keys, // Key to identify the item in the table
		}

		try {
			const command = new GetItemCommand(params) // Create GetItem command
			const res = await this.client.send(command) // Send the command to DynamoDB

			if (!res.Item) {
				// Handle case where the item is not found in the table
				if (this.dev) console.log('Item not found in database')
				throw new HttpError('Item not found in database', 404)
			}

			return res.Item // Return the retrieved item
		} catch (error) {
			if (this.dev) console.log('Error getting item from database: ', error)
			if (error instanceof HttpError) throw error
			throw new HttpError('Error getting item from database', 500)
		}
	}

	/**
	 * Get multiple items from a DynamoDB table in a batch request.
	 *
	 * @param {string} table - The name of the DynamoDB table.
	 * @param {Array} keys - An array of keys to identify the items in the table.
	 * @param {string|null} projection - Optional projection expression to specify which attributes to retrieve.
	 * @returns {Array} - An array of items retrieved from the table.
	 * @throws {HttpError} - Throws HttpError if there is an error retrieving the items.
	 */
	async batchGet(table, keys, projection = null) {
		const params = {
			RequestItems: {
				[table]: {
					Keys: keys,
				},
			},
		}

		if (projection) {
			const attributeNames = {}
			const placeholders = projection.split(',').map((attr) => {
				const trimmedAttr = attr.trim()
				const placeholder = `#${trimmedAttr}`
				attributeNames[placeholder] = trimmedAttr
				return placeholder
			})

			params.RequestItems[table].ProjectionExpression = placeholders.join(', ')
			params.RequestItems[table].ExpressionAttributeNames = attributeNames
		}

		try {
			const command = new BatchGetItemCommand(params)
			const res = await this.client.send(command)
			return res.Responses[table]
		} catch (error) {
			if (this.dev) console.log('Error getting batch items from database: ', error)
			throw new HttpError('Error getting batch items from database', 500)
		}
	}

	/**
	 * Write multiple items to a DynamoDB table in a batch request.
	 *
	 * @param {string} table - The name of the DynamoDB table.
	 * @param {Array} requests - A list of request objects containing the items to be written.
	 * @returns {Object} - The result of the batch write operation.
	 * @throws {HttpError} - Throws HttpError if there is an error writing the items.
	 */
	async batchWrite(table, requests) {
		const params = {
			RequestItems: {
				[table]: requests,
			},
		}

		try {
			const command = new BatchWriteItemCommand(params) // Create BatchWriteItem command
			const res = await this.client.send(command) // Send the command to DynamoDB

			if (res.UnprocessedItems[table]) {
				if (this.dev) console.log('Unprocessed items:', res.UnprocessedItems[table])
				throw new HttpError(`Items unprocessed during the batch writing`, 500)
			} else {
				if (this.dev) console.log('All items processed successfully')
			}

			return res
		} catch (error) {
			if (this.dev) console.log('Error batch writing items in database: ', error)
			if (error instanceof HttpError) throw error
			throw new HttpError('Error batch writing items in database', 500)
		}
	}

	/**
	 * Update an item in a DynamoDB table.
	 *
	 * @param {string} table - The name of the DynamoDB table.
	 * @param {Object} keys - The keys to identify the item in the table.
	 * @param {string} updateExpression - The update expression specifying the attributes to update.
	 * @param {Object} expressionAttributeNames - A mapping of attribute names for the update.
	 * @param {Object} expressionAttributeValues - A mapping of values for the update expression.
	 * @param {string|null} condition - Optional condition expression for conditional updates.
	 * @param {string} returnValues - What values to return after the update (e.g., 'ALL_NEW').
	 * @returns {Object} - The result of the update operation, including updated item values.
	 * @throws {HttpError} - Throws HttpError if there is an error updating the item.
	 */
	async update(table, keys, updateExpression, expressionAttributeNames, expressionAttributeValues, condition = null, returnValues = 'ALL_NEW') {
		const params = {
			TableName: table,
			Key: keys,
			UpdateExpression: updateExpression,
			ExpressionAttributeNames: expressionAttributeNames,
			ExpressionAttributeValues: expressionAttributeValues,
			ReturnValues: returnValues,
		}

		if (condition) {
			params.ConditionExpression = condition // Add condition expression if provided
		}

		try {
			const command = new UpdateItemCommand(params)
			const res = await this.client.send(command)
			return res
		} catch (error) {
			if (error.name === 'ConditionalCheckFailedException') {
				if (this.dev) console.log('Condition not met for update operation')
				throw new HttpError('Condition not met for update operation', 404)
			}
			if (this.dev) console.log('Error updating item in database: ', error)
			throw new HttpError('Error updating item in database', 500)
		}
	}

	/**
	 * Delete an item from a DynamoDB table.
	 *
	 * @param {string} table - The name of the DynamoDB table.
	 * @param {Object} keys - The keys to identify the item to delete.
	 * @param {string|null} condition - Optional condition expression for conditional delete.
	 * @returns {Object} - The keys of the item that was deleted.
	 * @throws {HttpError} - Throws HttpError if there is an error deleting the item.
	 */
	async delete(table, keys, condition = null) {
		const params = {
			TableName: table,
			Key: keys,
		}

		if (condition) {
			params.ConditionExpression = condition
		}

		try {
			const command = new DeleteItemCommand(params)
			await this.client.send(command)
			return keys
		} catch (error) {
			if (error.name === 'ConditionalCheckFailedException') {
				if (this.dev) console.log('Condition not met for delete operation')
				throw new HttpError('Condition not met for delete operation', 404)
			}
			if (this.dev) console.log('Error deleting item from database: ', error)
			throw new HttpError('Error deleting item from database', 500)
		}
	}

	/**
	 * Queries a DynamoDB table based on the provided key condition and optional filters.
	 *
	 * @param {string} table - The name of the DynamoDB table to query.
	 * @param {string} condition - The KeyConditionExpression to apply to the query.
	 * @param {Object} keys - A dictionary of key-value pairs to be used as ExpressionAttributeValues in the KeyConditionExpression.
	 * @param {string|null} [filter=null] - Optional. A FilterExpression to apply additional filtering on the results.
	 * @param {Object|null} [filterValues=null] - Optional. A dictionary of filter values to be used in the FilterExpression.
	 *
	 * @returns {Array} - An array of items returned from the query that match the conditions and filters.
	 *
	 * @throws {HttpError} - Throws an error if the query operation fails.
	 */
	async query(table, condition, keys, filter = null, filterValues = null) {
		const params = {
			TableName: table,
			KeyConditionExpression: condition,
			ExpressionAttributeValues: keys,
		}

		// If a filter and filter values are provided, add them to the query parameters
		if (filter && filterValues) {
			params.ExpressionAttributeValues = {
				...keys,
				...filterValues,
			}
			params.FilterExpression = filter

			// Generate placeholders for the filter values (e.g., #attrName)
			const placeholders = Object.keys(filterValues).map((key, _) => {
				return `#${key.slice(1)}`
			})

			const attributeNames = {}
			placeholders.forEach((placeholder, index) => {
				attributeNames[placeholder] = Object.keys(filterValues)[index].slice(1)
			})

			// Add the attribute names to the query parameters to handle reserved keywords
			params.ExpressionAttributeNames = attributeNames
		}

		try {
			const command = new QueryCommand(params)
			const res = await this.client.send(command)
			return res.Items
		} catch (error) {
			if (this.dev) console.log('Error querying item from database: ', error)
			throw new HttpError('Error querying item from database', 500)
		}
	}

	/**
	 * Queries a DynamoDB table using a Global Secondary Index (GSI) with the provided key condition and optional filters.
	 *
	 * @param {string} table - The name of the DynamoDB table to query.
	 * @param {string} indexName - The name of the Global Secondary Index to query.
	 * @param {string} condition - The KeyConditionExpression to apply to the query.
	 * @param {Object} keys - A dictionary of key-value pairs to be used as ExpressionAttributeValues in the KeyConditionExpression.
	 * @param {string|null} [filter=null] - Optional. A FilterExpression to apply additional filtering on the results.
	 * @param {Object|null} [filterValues=null] - Optional. A dictionary of filter values to be used in the FilterExpression.
	 *
	 * @returns {Array} - An array of items returned from the query that match the conditions and filters.
	 *
	 * @throws {HttpError} - Throws an error if the query operation fails.
	 */
	async queryByGSI(table, indexName, condition, attributeNames, keys, filter = null, filterValues = null) {
		const params = {
			TableName: table,
			IndexName: indexName,
			KeyConditionExpression: condition,
			ExpressionAttributeNames: attributeNames,
			ExpressionAttributeValues: keys,
		}

		if (filter && filterValues) {
			params.ExpressionAttributeValues = {
				...keys,
				...filterValues,
			}
			params.FilterExpression = filter

			params.ExpressionAttributeNames = {
				...attributeNames,
			}
			Object.keys(filterValues).forEach((key) => {
				const attributeName = key.slice(1)
				params.ExpressionAttributeNames[`#${attributeName}`] = attributeName
			})
		}

		try {
			const command = new QueryCommand(params) // Create a QueryCommand
			const res = await this.client.send(command) // Send the query
			return res.Items // Return the items from the response
		} catch (error) {
			if (this.dev) console.error('Error querying by GSI:', error)
			throw new HttpError('Error querying by GSI from database', error.name === 'ConditionalCheckFailedException' ? 400 : 500)
		}
	}

	/**
	 * Scans a DynamoDB table based on the provided filter expression and optional filter values.
	 *
	 * @param {string} table - The name of the DynamoDB table to scan.
	 * @param {string|null} [filter=null] - Optional. A FilterExpression to apply additional filtering on the results.
	 * @param {Object|null} [filterValues=null] - Optional. A dictionary of filter values to be used in the FilterExpression.
	 * @param {string|null} [projection=null] - Optional. A ProjectionExpression to specify which attributes to retrieve.
	 *
	 * @returns {Array} - An array of items returned from the scan that match the conditions and filters.
	 *
	 * @throws {HttpError} - Throws an error if the scan operation fails.
	 */
	async scan(table, filter = null, filterValues = null) {
		try {
			const params = {
				TableName: table,
			}

			if (filter && filterValues) {
				params.FilterExpression = filter
				params.ExpressionAttributeValues = filterValues

				const placeholders = Object.keys(filterValues).map((key, _) => {
					return `#${key.slice(1)}`
				})

				const attributeNames = {}
				placeholders.forEach((placeholder, index) => {
					attributeNames[placeholder] = Object.keys(filterValues)[index].slice(1)
				})

				params.ExpressionAttributeNames = attributeNames
			}

			const command = new ScanCommand(params)
			const res = await this.client.send(command)
			return res.Items
		} catch (error) {
			if (this.dev) console.log('Error scanning item from database: ', error)
			throw new HttpError('Error scanning item from database', 500)
		}
	}

	/**
	 * Clears a DynamoDB table by deleting all items in the table.
	 *
	 * @param {string} table - The name of the DynamoDB table to clear.
	 *
	 * @returns {Promise<void>}
	 *
	 * @throws {HttpError} - Throws an error if the table cannot be cleared.
	 */
	async clearTable(table) {
		try {
			const command = new ScanCommand({ TableName: table })
			const res = await this.client.send(command)
			const items = res.Items
			const requests = items.map((item) => {
				return {
					DeleteRequest: {
						Key: {
							id: item.id,
						},
					},
				}
			})
			await this.batchWrite(table, requests)
		} catch (error) {
			if (this.dev) console.log('Error clearing table in database: ', error)
			throw new HttpError('Error clearing table in database', 500)
		}
	}
}

module.exports = DynamoDB
