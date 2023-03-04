/*
 * Copyright 2022 Galactic Star Studios
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.galactic.star.tools;

import org.bukkit.Bukkit;
import org.bukkit.event.inventory.InventoryType;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.InventoryHolder;
import org.bukkit.inventory.ItemStack;

import java.io.Serializable;

/**
 * The Class used to serialize Bukkit Inventories and convert it to a byte array or to Base64.
 *
 * @author PrismoidNW
 */
public class InventorySerializer {

    /**
     * The default constructor. There is no need to instantiate this class.
     *
     * @throws InstantiationException When you instantiate this class.
     */
    public InventorySerializer() throws InstantiationException {
        throw new InstantiationException("You shouldn't instantiate this class as it is a utility class.");
    }

    /**
     * Converts the Bukkit Inventory into a byte array.
     *
     * @param inventory Bukkit Inventory.
     * @return Byte array.
     */
    public static byte[] serialize(Inventory inventory) {
        return ObjectSerializer.objectToByteArray(new StarInventory(inventory));
    }

    /**
     * Converts a Bukkit Inventory to Base64 String.
     *
     * @param inventory Bukkit Inventory.
     * @return String
     */
    public static String toBase64(Inventory inventory) {
        return Base64.encode(serialize(inventory));
    }


    /**
     * Converts a Base64 String into Bukkit inventory.
     *
     * @param data String.
     * @return Bukkit Inventory.
     */
    public static Inventory fromBase64(String data) {
        return deserialize(Base64.decode(data));
    }

    /**
     * Converts byte array to Bukkit inventory.
     *
     * @param data Byte array.
     * @return Bukkit inventory object.
     */
    public static Inventory deserialize(byte[] data) {
        return getInventory((StarInventory) ObjectSerializer.byteArrayToObject(data));
    }

    /**
     * Returns a Bukkit inventory from the StarInventory object.
     *
     * @param starInventory Serializable Bukkit Inventory.
     * @return Bukkit Inventory.
     */
    private static Inventory getInventory(StarInventory starInventory) {
        Inventory inventory;
        if (starInventory.size == 0) {
            inventory = Bukkit.createInventory(
                    starInventory.inventoryHolder,
                    starInventory.inventoryType,
                    starInventory.title
            );
        } else {
            inventory = Bukkit.createInventory(
                    starInventory.inventoryHolder,
                    starInventory.size,
                    starInventory.title
            );
        }
        inventory.setContents(starInventory.contents);
        inventory.setMaxStackSize(starInventory.maxStackSize);
        return inventory;
    }

    /**
     * The serializable Bukkit inventory implementation.
     *
     * @author PrismoidNW
     */
    private static class StarInventory implements Serializable {
        private final ItemStack[] contents;
        private final InventoryHolder inventoryHolder;
        private final int maxStackSize;
        private final int size;
        private final String title;
        private final InventoryType inventoryType;

        /**
         * The constructor used to set the inventory values so it can be serialized.
         *
         * @param inventory Bukkit Inventory.
         */
        public StarInventory(Inventory inventory) {
            this.contents = inventory.getContents();
            this.inventoryHolder = inventory.getHolder();
            this.maxStackSize = inventory.getMaxStackSize();
            this.size = inventory.getSize();
            this.title = inventory.getTitle();
            this.inventoryType = inventory.getType();
        }
    }
}
