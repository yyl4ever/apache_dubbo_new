/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.common.concurrent;

import java.util.Queue;

/**
 * A handler for rejected element that discards the oldest element.
 * 当队列满时，丢弃队列中最旧（最早进入队列）的元素，然后将新的元素添加到队列中
 * 当队列满时，通过丢弃队列中最旧的元素来为新元素腾出空间。
 */
public class DiscardOldestPolicy<E> implements Rejector<E> {

    @Override
    public void reject(final E e, final Queue<E> queue) {
        // 移除并返回队列头部的元素（最旧的元素）
        queue.poll();
        // 将新元素e添加到队列中
        queue.offer(e);
    }
}
