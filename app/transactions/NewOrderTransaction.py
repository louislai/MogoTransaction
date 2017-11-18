from Transaction import Transaction
from datetime import datetime
from decimal import *

class NewOrderTransaction(Transaction):

	def execute(self, params):
		# inputs
		w_id = int(params['w_id'])
		d_id = int(params['d_id'])
		c_id = int(params['c_id'])
		num_items = int(params['num_items'])
		if (num_items > 20):
			print "Number of items in this new order is greater than 20. Exit"
			return
		orders = map(lambda ol: map(int, ol), params['items'])

		# intermediate data
		w_tax = self.get_w_tax(w_id)
		customer = self.get_customer_for_output(w_id, d_id, c_id)

		# processing steps (follow sequence in project.pdf)
		next_o_id, d_tax = self.get_d_next_o_id_and_d_tax(w_id, d_id)
		self.update_d_next_o_id(w_id, d_id, next_o_id+1)
		entry_date = self.create_new_order(w_id, d_id, c_id, next_o_id, num_items, orders, customer)
		c_discount = customer.c_discount
		# print 'next o id', next_o_id
		print_item_results, total_amount = self.update_stock_and_create_order_line(w_id, d_id, c_id, next_o_id, orders, d_tax, w_tax, c_discount)
		self.print_output(w_id, d_id, c_id, customer, w_tax, d_tax, next_o_id, entry_date, num_items, total_amount)
		self.print_items(print_item_results)

	def get_w_tax(self, w_id):
		"""Get warehouse tax from the vertical partition collection warehouse-tax"""
		result = self.session['warehouse-tax'].find_one({'w_id': w_id},{ '_id': 0, 'w_tax': 1})
		if not result:
			print "Cannot find any warehouse with w_id {}".format(w_id)
			return
		else:
			return result['w_tax']

	def get_customer_for_output(self, w_id, d_id, c_id):
		"""Get customer info (c_first, c_middle, c_last, c_credit, c_discount) for printing output"""
		result = self.session['customer']\
			.find_one({'c_w_id': w_id, 'c_d_id': d_id, 'c_id': c_id},
					  {'_id': 0, 'c_first': 1, 'c_middle': 1, 'c_last': 1,
					   'c_credit': 1, 'c_discount': 1})
		if not result:
			print "Cannot find any customer with w_id d_id c_id {} {} {}".format(w_id, d_id, c_id)
			return
		else:
			return self.objectify(result)

	def get_d_next_o_id_and_d_tax(self, w_id, d_id):
		"""Get d_next_o_id and d_tax from vertical partition district-next-order-id"""
		result = self.session['district-next-order-id'].find_one({'d_w_id': w_id, 'd_id': d_id},
															  { '_id': 0, 'd_next_o_id': 1, 'd_tax': 1})
		if not result:
			print "Cannot find any district with w_id d_id {} {}".format(w_id, d_id)
			return
		else:
			return result['d_next_o_id'], result['d_tax']

	def update_d_next_o_id(self, w_id, d_id, new_d_next_o_id):
		"""Increment d_next_o_id of district-next-order-id collection"""
		self.session['district-next-order-id'].update_one({'d_id': d_id, 'd_w_id': w_id}, {'$set':{'d_next_o_id': new_d_next_o_id}})

	def get_all_local(self, w_id, orders):
		for (_, supply_warehouse_id, _) in orders:
			if w_id != supply_warehouse_id:
				return  0
		return 1

	def create_new_order(self, w_id, d_id, c_id, o_id, num_items, orders, customer):
		"""Create a new order (insert row into order-order-line collection)"""
		all_local = self.get_all_local(w_id, orders)
		t = datetime.strptime(datetime.utcnow().isoformat(' '), '%Y-%m-%d %H:%M:%S.%f')
		self.session['order-order-line'].insert({
			'o_w_id': w_id,
			'o_d_id': d_id,
			'o_id': o_id,
			'o_c_id': c_id,
			'o_carrier_id': -1,
			'o_ol_cnt': num_items,
			'o_all_local': all_local,
			'o_entry_d': t,
			'o_c_first': customer.c_first,
			'o_c_middle': customer.c_middle,
			'o_c_last': customer.c_last,
			'o_delivery_d': -1,
			'o_orderlines': []
		})
		return t

	def update_stock_and_create_order_line(self, w_id, d_id, c_id, n, orders, d_tax, w_tax, c_discount):
		"""Update stock collection, create a new order-line for each new item"""
		total_amount = 0
		result = []
		for index, (item_number, supplier_warehouse, quantity) in enumerate(orders):
			item_result = []
			item_result.append(item_number)
			row = self.session['stock'].find_one({'s_w_id': supplier_warehouse, 's_i_id': item_number},
												 {'_id': 0, 's_quantity': 1, 's_ytd': 1, 's_order_cnt': 1,
												  's_remote_cnt': 1, 's_i_price': 1, 's_i_name': 1})
			row = self.objectify(row)
			adjusted_qty = int(row.s_quantity) - quantity
			if adjusted_qty < 10:
				adjusted_qty += 100
			new_s_ytd = row.s_ytd + quantity
			new_s_order_cnt = row.s_order_cnt + 1
			if supplier_warehouse != w_id:
				counter = 1
			else:
				counter = 0
			new_s_remote_cnt = row.s_remote_cnt + counter
			self.session['stock'].update({'s_w_id': supplier_warehouse, 's_i_id': item_number},
											 {'$set':{'s_quantity': adjusted_qty,
													  's_ytd': new_s_ytd, 's_order_cnt': new_s_order_cnt,
													  's_remote_cnt': new_s_remote_cnt }}, multi=True)
			item_result.append(row.s_i_name)
			item_result.append(supplier_warehouse)
			item_result.append(quantity)
			item_amount = quantity * row.s_i_price
			item_result.append(item_amount)
			item_result.append(adjusted_qty)
			total_amount = total_amount + item_amount

			self.session['order-order-line'].update({ 'o_w_id': w_id, 'o_d_id': d_id, 'o_id': n }, {'$push': {'o_orderlines': {
				'ol_number': int(index) + 1,
				'ol_i_id': int(item_number),
				'ol_amount': item_amount,
				'ol_supply_w_id': supplier_warehouse,
				'ol_quantity': quantity,
				'ol_dist_info': 'S_DIST'+str(d_id),
				'ol_i_name': row.s_i_name
			}}})
			result.append(item_result)

		total_amount = total_amount * (1 + d_tax + w_tax) * (1 - c_discount)
		return result, total_amount

	def print_output(self, w_id, d_id, c_id, customer, w_tax, d_tax, o_id, entry_date, num_items, total_amount):
		"""Print output for the customer"""
		print
		print "Customer w_d_c_id:	{} {} {}".format(w_id, d_id, c_id)
		print "Customer lastname:	{}".format(customer.c_last)
		print "Credit:			{}".format(customer.c_credit)
		print "Discount:		{}".format(customer.c_discount)
		print "Warehouse tax:		{}".format(w_tax)
		print "District tax:		{}".format(d_tax)
		print "Order number:		{}".format(o_id)
		print "Entry date:		{}".format(entry_date)
		print "Number of items:	{}".format(num_items)
		print "Total amount:		{}".format(total_amount)

	def print_items(self, items):
		"""Print output for each item"""
		for (item_num, i_name, supplier_warehouse, quantity, ol_amount, s_quantity) in items:
			print
			print "Item number:		{}".format(item_num)
			print "Item name:		{}".format(i_name)
			print "Supplier warehouse:	{}".format(supplier_warehouse)
			print "Quantity:		{}".format(quantity)
			print "OL_amount:		{}".format(ol_amount)
			print "S_quantity:		{}".format(s_quantity)
