from Transaction import Transaction
from decimal import *

class PaymentTransaction(Transaction):

	def execute(self, params):
		# inputs
		c_w_id = int(params['c_w_id'])
		c_d_id = int(params['c_d_id'])
		c_id = int(params['c_id'])
		payment = float(params['payment'])

		# processing steps
		warehouse_print = self.update_warehouse(c_w_id, payment)
		district_print = self.update_district(c_w_id, c_d_id, payment)
		customer_print, new_c_balance = self.update_customer(c_w_id, c_d_id, c_id, payment)
		self.print_output(c_id, c_w_id, c_d_id, warehouse_print, district_print, customer_print, new_c_balance, payment)

	def update_warehouse(self, warehouse_id, payment):
		"""Increment w_ytd by payment, return warehouse print outputs"""
		row = self.session.warehouse.\
					find_one({ 'w_id': warehouse_id },
						 { '_id': 0, 'w_street_1': 1, 'w_street_2': 1, 'w_city': 1, 'w_state': 1, 'w_zip': 1, 'w_ytd': 1 })

		if not row:
			print "Cannot find any warehouse with w_id {}".format(warehouse_id)
			return
		else:
			row = self.objectify(row)
			new_w_ytd = float(row.w_ytd) + payment
			self.session.warehouse.update_one({ 'w_id': warehouse_id },  { '$set': { 'w_ytd': new_w_ytd }})
			return row

	def update_district(self, warehouse_id, district_id, payment):
		"""Increment d_ytd by payment, return district print outputs"""
		row = self.session.district\
					.find_one({ 'd_w_id': warehouse_id, 'd_id': district_id },
						  { '_id': 0, 'd_street_1': 1, 'd_street_2': 1, 'd_city': 1, 'd_state': 1, 'd_zip': 1, 'd_ytd': 1 })
		if not row:
			print "Cannot find any district with w_id d_id {} {}".format(warehouse_id, district_id)
			return
		else:
			row = self.objectify(row)
			new_d_ytd = float(row.d_ytd) + payment
			self.session.district.update_one({ 'd_w_id': warehouse_id, 'd_id': district_id }, {'$set': {'d_ytd': new_d_ytd}})
			return row

	def update_customer(self, warehouse_id, district_id, customer_id, payment):
		"""Decrease c_balance by payment, increase c_ytd_payment by payment, increment c_payment_cnt, return customer print outputs"""
		row = self.session.customer\
					.find_one({ 'c_w_id': warehouse_id, 'c_d_id': district_id, 'c_id': customer_id },
						  { '_id': 0, 'c_first': 1, 'c_middle': 1, 'c_last': 1, 'c_street_1': 1,
							'c_street_2': 1, 'c_city': 1, 'c_state': 1, 'c_zip': 1, 'c_phone': 1,
							'c_since': 1, 'c_credit': 1, 'c_discount': 1, 'c_credit_lim': 1,
							'c_balance': 1, 'c_ytd_payment': 1, 'c_payment_cnt': 1 })
		if not row:
			print "Cannot find any customer with c_id w_id d_id {} {} {}".format(customer_id, warehouse_id, district_id)
			return
		else:
			row = self.objectify(row)
			new_c_balance = row.c_balance - payment
			new_c_ytd_payment = row.c_ytd_payment + float(payment)
			new_c_payment_cnt = row.c_payment_cnt + 1
			self.session.customer.update_one({ 'c_w_id': warehouse_id, 'c_d_id': district_id, 'c_id': customer_id },
											 {'$set': { 'c_ytd_payment': new_c_ytd_payment, 'c_balance': new_c_balance,
														'c_payment_cnt': new_c_payment_cnt }})
			return row, new_c_balance

	def print_output(self, customer_id, warehouse_id, district_id, warehouse, district, customer, c_balance, payment):
		"""Print required outputs"""
		print
		print "Customer w_d_c_id:	{} {} {}".format(warehouse_id, district_id, customer_id)
		print "Customer name:		{} {} {}".format(customer.c_first, customer.c_middle, customer.c_last)
		print "Address:		{} {} {} {} {}".format(customer.c_street_1, customer.c_street_2, customer.c_city, customer.c_state, customer.c_zip)
		print "Phone:			{}".format(customer.c_phone)
		print "Since:			{}".format(customer.c_since)
		print "Credit:			{} with limit {}".format(customer.c_credit, customer.c_credit_lim)
		print "Discount:		{}".format(customer.c_discount)
		print "Balance:		{}".format(c_balance)
		print "Warehouse:		{} {} {} {} {}".format(warehouse.w_street_1, warehouse.w_street_2, warehouse.w_city, warehouse.w_state, warehouse.w_zip)
		print "District:		{} {} {} {} {}".format(district.d_street_1, district.d_street_2, district.d_city, district.d_state, district.d_zip)
		print "Payment:		{}".format(payment)