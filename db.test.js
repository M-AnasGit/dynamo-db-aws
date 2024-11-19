const DynamoDB = require('./DynamoDB')

describe('DynamoDB', () => {
	let db

	beforeAll(() => {
		db = new DynamoDB({
			region: 'eu-north-1',
		})
	})

	afterAll(async () => {
		await db.clearTable('test')
	})

	describe('set', () => {
		test('set item', async () => {
			const item = await db.set('test', {
				id: { S: 'test1' },
				name: { S: 'John' },
			})
			expect(item).toEqual({ id: { S: 'test1' }, name: { S: 'John' } })
		})

		test('set item with condition', async () => {
			await db.set('test', { id: { S: 'test2' }, name: { S: 'Jane' } })
			await expect(db.set('test', { id: { S: 'test2' }, name: { S: 'Jane' } }, 'attribute_not_exists(id)')).rejects.toThrow(
				'Item already exists in database'
			)
		})
	})

	describe('get', () => {
		test('get item', async () => {
			const item = await db.get('test', { id: { S: 'test1' } })
			expect(item).toEqual({ id: { S: 'test1' }, name: { S: 'John' } })
		})

		test('get item not found', async () => {
			await expect(db.get('test', { id: { S: 'test3' } })).rejects.toThrow('Item not found in database')
		})
	})

	describe('batch-get', () => {
		test('get items', async () => {
			const items = await db.batchGet('test', [{ id: { S: 'test1' } }, { id: { S: 'test2' } }])
			expect(items.length).toEqual(2)
		})

		test('get items not found', async () => {
			const items = await db.batchGet('test', [{ id: { S: 'test1' } }, { id: { S: 'test3' } }])
			await expect(items).toEqual([{ id: { S: 'test1' }, name: { S: 'John' } }])
		})
	})

	describe('batchWrite', () => {
		test('batch write and delete items successfully', async () => {
			const items = [{ PutRequest: { Item: { id: { S: 'test3' }, name: { S: 'John' } } } }, { DeleteRequest: { Key: { id: { S: 'test2' } } } }]

			await db.batchWrite('test', items)

			const item1 = await db.get('test', { id: { S: 'test3' } })
			expect(item1).toEqual({ id: { S: 'test3' }, name: { S: 'John' } })

			await expect(db.get('test', { id: { S: 'test2' } })).rejects.toThrow('Item not found in database')
		})

		test('batch write with put operation only', async () => {
			const items = [
				{ PutRequest: { Item: { id: { S: 'test4' }, name: { S: 'John' } } } },
				{ PutRequest: { Item: { id: { S: 'test5' }, name: { S: 'Jane' } } } },
			]

			await db.batchWrite('test', items)

			const item = await db.get('test', { id: { S: 'test4' } })
			expect(item).toEqual({ id: { S: 'test4' }, name: { S: 'John' } })
		})

		test('batch write with delete operation only', async () => {
			const items = [{ DeleteRequest: { Key: { id: { S: 'test3' } } } }, { DeleteRequest: { Key: { id: { S: 'test4' } } } }]

			await db.batchWrite('test', items)

			await expect(db.get('test', { id: { S: 'test3' } })).rejects.toThrow('Item not found in database')
			await expect(db.get('test', { id: { S: 'test4' } })).rejects.toThrow('Item not found in database')
		})
	})

	describe('update', () => {
		test('update item', async () => {
			await db.update('test', { id: { S: 'test1' } }, 'set #name = :name', { '#name': 'name' }, { ':name': { S: 'Jane' } })
			const item = await db.get('test', { id: { S: 'test1' } })
			expect(item).toEqual({ id: { S: 'test1' }, name: { S: 'Jane' } })

			const new_item = await db.update('test', { id: { S: 'test1' } }, 'set #name = :name', { '#name': 'name' }, { ':name': { S: 'Jonathan' } })
			expect(new_item.Attributes).toEqual({ id: { S: 'test1' }, name: { S: 'Jonathan' } })
		})
		test('update item with condition', async () => {
			const item = await db.update(
				'test',
				{ id: { S: 'test1' } },
				'set #name = :name',
				{ '#name': 'name' },
				{ ':name': { S: 'Jane' }, ':expectedName': { S: 'Jonathan' } },
				'#name = :expectedName'
			)

			expect(item.Attributes).toEqual({ id: { S: 'test1' }, name: { S: 'Jane' } })
		})
		test('update item with condition not met', async () => {
			await expect(
				db.update(
					'test',
					{ id: { S: 'test1' } },
					'set #name = :name',
					{ '#name': 'name' },
					{ ':name': { S: 'Jane' }, ':expectedName': { S: 'Not_Jonathan' } },
					'#name = :expectedName'
				)
			).rejects.toThrow('Condition not met for update operation')
		})
		test('update item not found', async () => {
			await expect(
				db.update('test', { id: { S: 'test14' } }, 'set #name = :name', { '#name': 'name' }, { ':name': { S: 'Jane' } }, 'attribute_exists(id)')
			).rejects.toThrow('Condition not met for update operation')
		})
	})

	describe('query', () => {
		test('query items', async () => {
			const items = await db.query('test', 'id = :id', { ':id': { S: 'test1' } })
			expect(items.length).toEqual(1)
		})
		test('query items with condition', async () => {
			const items = await db.query('test', 'id = :id', { ':id': { S: 'test1' } }, '#name = :name', { ':name': { S: 'Jane' } })
			expect(items.length).toEqual(1)
		})
		test('query items with condition not met', async () => {
			const items = await db.query('test', 'id = :id', { ':id': { S: 'test1' } }, '#name = :name', { ':name': { S: 'Jonathan' } })
			expect(items.length).toEqual(0)
		})
		test('query items with GSI', async () => {
			const items = await db.queryByGSI('test', 'name-index', '#name = :name', { '#name': 'name' }, { ':name': { S: 'Jane' } })
			expect(items.length).toEqual(2)
		})
		test('query items not found', async () => {
			const items = await db.query('test', 'id = :id', { ':id': { S: 'test73' } })
			expect(items.length).toEqual(0)
		})
	})

	describe('delete', () => {
		test('delete item', async () => {
			await db.delete('test', { id: { S: 'test1' } })
			await expect(db.get('test', { id: { S: 'test1' } })).rejects.toThrow('Item not found in database')
		})
		test('delete item with condition', async () => {
			await db.set('test', { id: { S: 'test2' }, name: { S: 'Jane' } })
			await db.delete('test', { id: { S: 'test2' } }, 'attribute_exists(id)')
			await expect(db.get('test', { id: { S: 'test2' } })).rejects.toThrow('Item not found in database')
		})
		test('delete item with condition not met', async () => {
			await db.set('test', { id: { S: 'test3' }, name: { S: 'Jhon' } })
			await expect(db.delete('test', { id: { S: 'test3' } }, 'attribute_not_exists(id)')).rejects.toThrow('Condition not met for delete operation')
		})
		test('delete item not found', async () => {
			await expect(db.delete('test', { id: { S: 'test74' } }, 'attribute_exists(id)')).rejects.toThrow('Condition not met for delete operation')
		})
	})
})
