
class Vehicle:
    def __init__(self, brand, model, type):
        self.brand = brand
        self.model = model
        self.type = type
        self.gas_tank_size = 14
        self.fuel_level = 0

    def fuel_up(self):
        self.fuel_level = self.gas_tank_size
        print('Gas tank is now full.')

    def drive(self):
        print(f'The {self.model} is now driving.')

    def update_fuel_level(self, new_level):
        if new_level <= self.gas_tank_size:
            self.fuel_level = new_level
        else:
            print('Exceeded capacity')

a = Vehicle('Tesla', 'Model Y', 'electric')
# # a.fuel_up()
# a.fuel_up()
# a.update_fuel_level(a.fuel_level)

# how to print the all the attributes of the class
print(a.__dict__)



